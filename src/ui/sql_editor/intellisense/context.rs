impl SqlEditorWidget {
    fn bounded_text_window(
        buffer: &TextBuffer,
        text_shadow: &Arc<Mutex<HighlightShadowState>>,
        start: i32,
        end: i32,
    ) -> (String, i32) {
        text_buffer_access::bounded_text_window(buffer, Some(text_shadow), start, end)
    }

    fn word_at_cursor(
        buffer: &TextBuffer,
        text_shadow: &Arc<Mutex<HighlightShadowState>>,
        cursor_pos: i32,
    ) -> (String, usize, usize) {
        let buffer_len = buffer.length().max(0);
        if buffer_len == 0 {
            return (String::new(), 0, 0);
        }
        let cursor_pos = cursor_pos.clamp(0, buffer_len);
        let start = (cursor_pos - INTELLISENSE_WORD_WINDOW).max(0);
        let end = (cursor_pos + INTELLISENSE_WORD_WINDOW).min(buffer_len);
        let (text, start) = Self::bounded_text_window(buffer, text_shadow, start, end);
        if text.is_empty() {
            let cursor = cursor_pos.max(0) as usize;
            return (String::new(), cursor, cursor);
        }
        let rel_cursor =
            Self::clamp_to_char_boundary_local(&text, (cursor_pos - start).max(0) as usize);
        let (word, rel_start, rel_end) = get_word_at_cursor(&text, rel_cursor);
        let abs_start = start as usize + rel_start;
        let abs_end = start as usize + rel_end;
        (word, abs_start, abs_end)
    }

    fn quoted_identifier_bounds_at(text: &str, rel_pos: usize) -> Option<(usize, usize)> {
        if text.is_empty() {
            return None;
        }

        let rel_pos = Self::clamp_to_char_boundary_local(text, rel_pos.min(text.len()));
        let mut idx = 0usize;

        while idx < text.len() {
            let ch = text.get(idx..)?.chars().next()?;
            if ch != '"' {
                idx += ch.len_utf8();
                continue;
            }

            let start = idx;
            idx += 1;

            while idx < text.len() {
                let cur = text.get(idx..)?.chars().next()?;
                if cur == '"' {
                    let next_idx = idx + cur.len_utf8();
                    if next_idx < text.len() && text.get(next_idx..)?.starts_with('"') {
                        idx = next_idx + 1;
                        continue;
                    }
                    let end = next_idx;
                    if rel_pos >= start && rel_pos <= end {
                        return Some((start, end));
                    }
                    idx = end;
                    break;
                }
                idx += cur.len_utf8();
            }

            if idx >= text.len() && rel_pos >= start && rel_pos <= text.len() {
                return Some((start, text.len()));
            }
        }

        None
    }

    fn identifier_at_position_in_text(
        text: &str,
        rel_pos: usize,
    ) -> Option<(String, usize, usize)> {
        if text.is_empty() {
            return None;
        }

        let rel_pos = Self::clamp_to_char_boundary_local(text, rel_pos.min(text.len()));

        if let Some((start, end)) = Self::quoted_identifier_bounds_at(text, rel_pos) {
            let raw = text.get(start..end)?;
            let word = Self::strip_identifier_quotes(raw);
            if !word.is_empty() {
                return Some((word, start, end));
            }
        }

        let anchor = if rel_pos < text.len() {
            let ch = text.get(rel_pos..)?.chars().next()?;
            if sql_text::is_identifier_char(ch) {
                Some(rel_pos)
            } else {
                None
            }
        } else {
            None
        }
        .or_else(|| {
            if rel_pos == 0 {
                None
            } else {
                text.get(..rel_pos)
                    .and_then(|prefix| prefix.char_indices().next_back())
                    .and_then(|(prev_start, ch)| {
                        if sql_text::is_identifier_char(ch) {
                            Some(prev_start)
                        } else {
                            None
                        }
                    })
            }
        })?;

        let mut start = anchor;
        while start > 0 {
            let Some((prev_start, ch)) = text
                .get(..start)
                .and_then(|prefix| prefix.char_indices().next_back())
            else {
                break;
            };
            if sql_text::is_identifier_char(ch) {
                start = prev_start;
            } else {
                break;
            }
        }

        let mut end = anchor;
        while end < text.len() {
            let Some(ch) = text.get(end..).and_then(|suffix| suffix.chars().next()) else {
                break;
            };
            if sql_text::is_identifier_char(ch) {
                end += ch.len_utf8();
            } else {
                break;
            }
        }

        let word = text.get(start..end)?.to_string();
        if word.is_empty() {
            None
        } else {
            Some((word, start, end))
        }
    }

    fn identifier_at_position(
        buffer: &TextBuffer,
        text_shadow: &Arc<Mutex<HighlightShadowState>>,
        pos: i32,
    ) -> Option<(String, i32, i32)> {
        let buffer_len = buffer.length().max(0);
        if buffer_len == 0 {
            return None;
        }
        let pos = pos.clamp(0, buffer_len);
        let line_start = text_buffer_access::line_start(buffer, Some(text_shadow), pos).max(0);
        let line_end = text_buffer_access::line_end(buffer, Some(text_shadow), pos).max(line_start);
        let text = text_buffer_access::text_range(buffer, Some(text_shadow), line_start, line_end);
        if text.is_empty() {
            return None;
        }

        let rel_pos = (pos - line_start).max(0) as usize;
        let (word, start, end) = Self::identifier_at_position_in_text(&text, rel_pos)?;
        Some((word, line_start + start as i32, line_start + end as i32))
    }

    fn quick_describe_type_priority(object_type: &str) -> i32 {
        match object_type.to_uppercase().as_str() {
            "TABLE" => 0,
            "VIEW" => 1,
            "FUNCTION" => 2,
            "PROCEDURE" => 3,
            "SEQUENCE" => 4,
            "PACKAGE" => 5,
            "PACKAGE BODY" => 6,
            _ => 50,
        }
    }

    fn format_argument_type_for_quick_describe(arg: &ProcedureArgument) -> String {
        if let Some(pls_type) = arg.pls_type.as_deref() {
            let trimmed = pls_type.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }

        if let Some(data_type) = arg.data_type.as_deref() {
            let upper = data_type.trim().to_uppercase();
            if upper == "NUMBER" {
                if let (Some(p), Some(s)) = (arg.data_precision, arg.data_scale) {
                    return format!("NUMBER({},{})", p, s);
                }
                if let Some(p) = arg.data_precision {
                    return format!("NUMBER({})", p);
                }
                return "NUMBER".to_string();
            }

            if matches!(
                upper.as_str(),
                "VARCHAR2" | "NVARCHAR2" | "VARCHAR" | "CHAR" | "NCHAR" | "RAW"
            ) {
                if let Some(len) = arg.data_length {
                    return format!("{}({})", upper, len.max(1));
                }
                return upper;
            }

            return upper;
        }

        if let Some(type_name) = arg.type_name.as_deref() {
            if let Some(owner) = arg.type_owner.as_deref() {
                return format!("{}.{}", owner, type_name);
            }
            return type_name.to_string();
        }

        "UNKNOWN".to_string()
    }

    fn format_routine_details(
        qualified_name: &str,
        routine_type: &str,
        arguments: &[ProcedureArgument],
    ) -> String {
        let mut details = format!(
            "=== {} {} ===\n\n",
            routine_type.to_uppercase(),
            qualified_name.to_uppercase()
        );

        if arguments.is_empty() {
            details.push_str("No argument metadata found.\n");
            return details;
        }

        let selected_overload = arguments.first().and_then(|arg| arg.overload);
        let selected: Vec<&ProcedureArgument> = arguments
            .iter()
            .filter(|arg| arg.overload == selected_overload)
            .collect();

        if let Some(overload) = selected_overload {
            details.push_str(&format!("Overload: {}\n\n", overload));
        }

        details.push_str(&format!(
            "{:<24} {:<12} {}\n",
            "Argument", "Direction", "Type"
        ));
        details.push_str(&format!("{}\n", "-".repeat(72)));

        let mut return_type: Option<String> = None;
        for arg in selected {
            let is_return = arg.position == 0 && arg.name.is_none();
            let type_display = Self::format_argument_type_for_quick_describe(arg);
            if is_return {
                return_type = Some(type_display);
                continue;
            }
            let arg_name = arg
                .name
                .clone()
                .unwrap_or_else(|| format!("ARG{}", arg.position.max(1)));
            let direction = arg.in_out.clone().unwrap_or_else(|| "IN".to_string());
            details.push_str(&format!(
                "{:<24} {:<12} {}\n",
                arg_name, direction, type_display
            ));
        }

        if let Some(return_type) = return_type {
            details.push_str(&format!("\nReturn Type: {}\n", return_type));
        }

        details
    }

    fn format_sequence_details(info: &SequenceInfo) -> String {
        let mut details = format!("=== Sequence Info: {} ===\n\n", info.name.to_uppercase());
        details.push_str(&format!("{:<18} {}\n", "Min Value", info.min_value));
        details.push_str(&format!("{:<18} {}\n", "Max Value", info.max_value));
        details.push_str(&format!("{:<18} {}\n", "Increment By", info.increment_by));
        details.push_str(&format!("{:<18} {}\n", "Cycle", info.cycle_flag));
        details.push_str(&format!("{:<18} {}\n", "Order", info.order_flag));
        details.push_str(&format!("{:<18} {}\n", "Cache Size", info.cache_size));
        details.push_str(&format!("{:<18} {}\n", "Last Number", info.last_number));
        details.push_str("\nNote: LAST_NUMBER is the next value to be generated.\n");
        details
    }

    fn describe_object(
        conn: &Connection,
        object_name: &str,
        qualifier: Option<&str>,
    ) -> Result<QuickDescribeData, String> {
        let object_name_upper = object_name.to_uppercase();

        if let Some(package_name) = qualifier {
            let package_name_upper = package_name.to_uppercase();
            if let Ok(routines) = ObjectBrowser::get_package_routines(conn, &package_name_upper) {
                if let Some(routine) = routines
                    .iter()
                    .find(|routine| routine.name.eq_ignore_ascii_case(&object_name_upper))
                {
                    let args = ObjectBrowser::get_package_procedure_arguments(
                        conn,
                        &package_name_upper,
                        &object_name_upper,
                    )
                    .map_err(|err| err.to_string())?;
                    let qualified_name = format!("{}.{}", package_name_upper, object_name_upper);
                    let content =
                        Self::format_routine_details(&qualified_name, &routine.routine_type, &args);
                    return Ok(QuickDescribeData::Text {
                        title: format!(
                            "Describe: {} ({})",
                            qualified_name,
                            routine.routine_type.to_uppercase()
                        ),
                        content,
                    });
                }
            }
        }

        if let Ok(columns) = ObjectBrowser::get_table_structure(conn, &object_name_upper) {
            if !columns.is_empty() {
                return Ok(QuickDescribeData::TableColumns(columns));
            }
        }

        let mut object_types = ObjectBrowser::get_object_types(conn, &object_name_upper)
            .map_err(|err| err.to_string())?;
        if object_types.is_empty() {
            return Err(format!(
                "Object not found or not accessible: {}",
                object_name_upper
            ));
        }

        object_types.sort_by_key(|object_type| Self::quick_describe_type_priority(object_type));

        for object_type in object_types {
            let object_type_upper = object_type.to_uppercase();
            match object_type_upper.as_str() {
                "TABLE" | "VIEW" => {
                    if let Ok(columns) =
                        ObjectBrowser::get_table_structure(conn, &object_name_upper)
                    {
                        if !columns.is_empty() {
                            return Ok(QuickDescribeData::TableColumns(columns));
                        }
                    }
                }
                "FUNCTION" | "PROCEDURE" => {
                    let args = ObjectBrowser::get_procedure_arguments(conn, &object_name_upper)
                        .unwrap_or_default();
                    let content =
                        Self::format_routine_details(&object_name_upper, &object_type_upper, &args);
                    return Ok(QuickDescribeData::Text {
                        title: format!("Describe: {} ({})", object_name_upper, object_type_upper),
                        content,
                    });
                }
                "SEQUENCE" => {
                    if let Ok(info) = ObjectBrowser::get_sequence_info(conn, &object_name_upper) {
                        return Ok(QuickDescribeData::Text {
                            title: format!("Describe: {} (SEQUENCE)", object_name_upper),
                            content: Self::format_sequence_details(&info),
                        });
                    }
                }
                "PACKAGE" => {
                    if let Ok(ddl) = ObjectBrowser::get_package_spec_ddl(conn, &object_name_upper) {
                        return Ok(QuickDescribeData::Text {
                            title: format!("Describe: {} (PACKAGE)", object_name_upper),
                            content: ddl,
                        });
                    }
                }
                _ => {
                    if let Ok(ddl) =
                        ObjectBrowser::get_object_ddl(conn, &object_type_upper, &object_name_upper)
                    {
                        return Ok(QuickDescribeData::Text {
                            title: format!(
                                "Describe: {} ({})",
                                object_name_upper, object_type_upper
                            ),
                            content: ddl,
                        });
                    }
                }
            }
        }

        Err(format!(
            "Object not found or not accessible: {}",
            object_name_upper
        ))
    }

    fn context_before_cursor(
        buffer: &TextBuffer,
        text_shadow: &Arc<Mutex<HighlightShadowState>>,
        cursor_pos: i32,
    ) -> String {
        let buffer_len = buffer.length().max(0);
        let cursor_pos = cursor_pos.clamp(0, buffer_len);
        let start = (cursor_pos - INTELLISENSE_CONTEXT_WINDOW).max(0);
        let (window, window_start) =
            Self::bounded_text_window(buffer, text_shadow, start, cursor_pos);
        if window.is_empty() {
            return String::new();
        }

        let mut rel_cursor = (cursor_pos - window_start).max(0) as usize;
        if rel_cursor > window.len() {
            rel_cursor = window.len();
        }
        let rel_cursor = Self::clamp_to_char_boundary_local(&window, rel_cursor);
        let before_cursor = window.get(..rel_cursor).unwrap_or("");
        let (stmt_start, _) = Self::statement_bounds_in_text(before_cursor, before_cursor.len());
        before_cursor.get(stmt_start..).unwrap_or("").to_string()
    }

    fn clamp_to_char_boundary_local(text: &str, idx: usize) -> usize {
        let mut idx = idx.min(text.len());
        if text.is_char_boundary(idx) {
            return idx;
        }

        // Clamp invalid UTF-8 byte offsets to the previous valid boundary.
        while idx > 0 && !text.is_char_boundary(idx) {
            idx -= 1;
        }
        idx
    }

    fn raw_cursor_position(buffer: &TextBuffer, pos: i32) -> i32 {
        let buffer_len = buffer.length().max(0);
        pos.clamp(0, buffer_len)
    }

    fn raw_cursor_byte_offset(pos: i32, buffer_len: i32) -> usize {
        pos.clamp(0, buffer_len.max(0)) as usize
    }

    pub(super) fn cursor_position(buffer: &TextBuffer, pos: i32) -> (i32, usize) {
        let buffer_len = buffer.length().max(0);
        let cursor_pos = Self::raw_cursor_position(buffer, pos);
        let cursor_byte = Self::raw_cursor_byte_offset(cursor_pos, buffer_len);
        (cursor_pos, cursor_byte)
    }

    pub(super) fn editor_cursor_position(editor: &TextEditor, buffer: &TextBuffer) -> (i32, usize) {
        Self::cursor_position(buffer, editor.insert_position())
    }

    fn statement_window_with_cursor(
        buffer: &TextBuffer,
        text_shadow: &Arc<Mutex<HighlightShadowState>>,
        cursor_pos: i32,
    ) -> (String, usize) {
        let buffer_len = buffer.length().max(0);
        if buffer_len == 0 {
            return (String::new(), 0);
        }
        let cursor_pos = cursor_pos.clamp(0, buffer_len);
        let start_candidate = (cursor_pos - INTELLISENSE_STATEMENT_WINDOW).max(0);
        let end_candidate = (cursor_pos + INTELLISENSE_STATEMENT_WINDOW).min(buffer_len);
        let (text, start) =
            Self::bounded_text_window(buffer, text_shadow, start_candidate, end_candidate);
        if text.is_empty() {
            return (String::new(), 0);
        }
        let mut rel_cursor = (cursor_pos - start).max(0) as usize;
        if rel_cursor > text.len() {
            rel_cursor = text.len();
        }
        rel_cursor = Self::clamp_to_char_boundary_local(&text, rel_cursor);
        (text, rel_cursor)
    }

    #[cfg(test)]
    fn statement_context_in_text(text: &str, cursor_pos: usize) -> String {
        if text.is_empty() {
            return String::new();
        }
        let cursor_pos = cursor_pos.min(text.len());
        let start_candidate = cursor_pos.saturating_sub(INTELLISENSE_STATEMENT_WINDOW as usize);
        let end_candidate = cursor_pos
            .saturating_add(INTELLISENSE_STATEMENT_WINDOW as usize)
            .min(text.len());
        let bytes = text.as_bytes();
        let start = bytes[..start_candidate]
            .iter()
            .rposition(|&b| b == b'\n')
            .map(|idx| idx + 1)
            .unwrap_or(0);
        let end = bytes[end_candidate..]
            .iter()
            .position(|&b| b == b'\n')
            .map(|idx| end_candidate + idx)
            .unwrap_or(text.len());
        let window = text.get(start..end).unwrap_or("");
        let rel_cursor = cursor_pos.saturating_sub(start).min(window.len());
        let (stmt_start, stmt_end) = Self::statement_bounds_in_text(window, rel_cursor);
        window.get(stmt_start..stmt_end).unwrap_or("").to_string()
    }

    #[cfg(test)]
    fn context_before_cursor_in_text(text: &str, cursor_pos: usize) -> String {
        let cursor_pos = Self::clamp_to_char_boundary_local(text, cursor_pos.min(text.len()));
        let start = cursor_pos.saturating_sub(INTELLISENSE_CONTEXT_WINDOW as usize);
        let start = Self::clamp_to_char_boundary_local(text, start);
        let window = text.get(start..cursor_pos).unwrap_or("");
        let (stmt_start, _) = Self::statement_bounds_in_text(window, window.len());
        window.get(stmt_start..).unwrap_or("").to_string()
    }

    fn should_skip_leading_intellisense_context_line(line: &str) -> bool {
        let trimmed = line.trim();
        trimmed.is_empty() || trimmed.starts_with("--") || Self::is_sqlplus_command_line(trimmed)
    }

    fn normalize_intellisense_context(
        text: &str,
        cursor_byte: usize,
    ) -> NormalizedIntellisenseContext {
        let cursor_byte = Self::clamp_to_char_boundary_local(text, cursor_byte.min(text.len()));
        let before_cursor = text.get(..cursor_byte).unwrap_or("");
        let stripped_cursor = Self::strip_sqlplus_prompt_prefixes(before_cursor).len();
        let text = Self::strip_sqlplus_prompt_prefixes(text);
        let cursor_byte =
            Self::clamp_to_char_boundary_local(&text, stripped_cursor.min(text.len()));
        let mut normalized = String::with_capacity(text.len());
        let mut raw_offset = 0usize;
        let mut normalized_cursor = 0usize;
        let mut cursor_recorded = false;
        let mut skipping_prefix = true;

        for segment in text.split_inclusive('\n') {
            let segment_start = raw_offset;
            raw_offset += segment.len();

            let (line, line_end) = if let Some(stripped) = segment.strip_suffix('\n') {
                (stripped, "\n")
            } else {
                (segment, "")
            };

            if skipping_prefix && Self::should_skip_leading_intellisense_context_line(line) {
                if !cursor_recorded && cursor_byte <= raw_offset {
                    normalized_cursor = normalized.len();
                    cursor_recorded = true;
                }
                continue;
            }
            skipping_prefix = false;

            if !cursor_recorded && cursor_byte <= raw_offset {
                let cursor_in_segment = cursor_byte.saturating_sub(segment_start).min(segment.len());
                let cursor_in_line = cursor_in_segment.min(line.len());
                normalized_cursor = normalized.len() + cursor_in_line;
                cursor_recorded = true;
            }

            normalized.push_str(line);
            normalized.push_str(line_end);
        }

        if !cursor_recorded {
            normalized_cursor = normalized.len();
        }

        let normalized_cursor = Self::clamp_to_char_boundary_local(
            &normalized,
            normalized_cursor.min(normalized.len()),
        );
        NormalizedIntellisenseContext {
            text: normalized,
            cursor_byte: normalized_cursor,
        }
    }

    fn normalize_intellisense_context_text(text: &str) -> String {
        Self::normalize_intellisense_context(text, text.len()).text
    }

    fn normalize_intellisense_context_with_cursor(
        text: &str,
        cursor_byte: usize,
    ) -> (String, usize) {
        let normalized = Self::normalize_intellisense_context(text, cursor_byte);
        (normalized.text, normalized.cursor_byte)
    }

    fn strip_sqlplus_prompt_prefixes(text: &str) -> String {
        let mut normalized = String::with_capacity(text.len());
        let mut saw_sql_prompt = false;

        for segment in text.split_inclusive('\n') {
            let (line, line_end) = if let Some(stripped) = segment.strip_suffix('\n') {
                (stripped, "\n")
            } else {
                (segment, "")
            };

            let stripped_line = if let Some(stripped) = Self::strip_sqlplus_sql_prompt_prefix(line)
            {
                saw_sql_prompt = true;
                stripped
            } else if saw_sql_prompt {
                Self::strip_sqlplus_numbered_prompt_prefix(line).unwrap_or(line)
            } else {
                line
            };
            normalized.push_str(stripped_line);
            normalized.push_str(line_end);
        }

        normalized
    }

    fn strip_sqlplus_sql_prompt_prefix(line: &str) -> Option<&str> {
        let bytes = line.as_bytes();
        let mut idx = 0usize;

        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }

        if bytes.get(idx..idx + 4).is_some_and(|slice| {
            slice[0].eq_ignore_ascii_case(&b'S')
                && slice[1].eq_ignore_ascii_case(&b'Q')
                && slice[2].eq_ignore_ascii_case(&b'L')
                && slice[3] == b'>'
        }) {
            idx += 4;
            while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                idx += 1;
            }
            return Some(&line[idx..]);
        }

        None
    }

    fn strip_sqlplus_numbered_prompt_prefix(line: &str) -> Option<&str> {
        let bytes = line.as_bytes();
        let mut idx = 0usize;

        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }

        let number_start = idx;
        let had_leading_whitespace = number_start > 0;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            idx += 1;
        }
        if had_leading_whitespace && idx > number_start {
            let mut sep = idx;
            while sep < bytes.len() && bytes[sep].is_ascii_whitespace() {
                sep += 1;
            }
            let whitespace_count = sep.saturating_sub(idx);
            if whitespace_count >= 2 {
                return Some(&line[sep..]);
            }
        }

        None
    }

    fn is_sqlplus_command_line(trimmed_line: &str) -> bool {
        crate::ui::sql_editor::query_text::is_sqlplus_command_line(trimmed_line)
    }

    // 문장 경계 계산은 실행/포맷 공통 규칙을 공유하기 위해 `query_text` 유틸을 사용합니다.
    fn statement_bounds_in_text(text: &str, cursor_pos: usize) -> (usize, usize) {
        crate::ui::sql_editor::query_text::statement_bounds_in_text(text, cursor_pos)
    }

    fn strip_identifier_quotes(value: &str) -> String {
        let trimmed = value.trim();
        if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
            trimmed[1..trimmed.len() - 1].replace("\"\"", "\"")
        } else {
            trimmed.to_string()
        }
    }

    fn qualifier_before_word(
        buffer: &TextBuffer,
        text_shadow: &Arc<Mutex<HighlightShadowState>>,
        word_start: usize,
    ) -> Option<String> {
        if word_start == 0 {
            return None;
        }
        let buffer_len = buffer.length().max(0) as usize;
        if word_start > buffer_len {
            return None;
        }
        let start = word_start
            .saturating_sub(INTELLISENSE_QUALIFIER_WINDOW as usize)
            .min(word_start);
        let (text, start) = Self::bounded_text_window(
            buffer,
            text_shadow,
            start as i32,
            (word_start as i32).max(0),
        );
        let mut rel_word_start = (word_start as i32 - start).max(0) as usize;
        if rel_word_start > text.len() {
            rel_word_start = text.len();
        }
        rel_word_start = Self::clamp_to_char_boundary_local(&text, rel_word_start);
        Self::qualifier_before_word_in_text(&text, rel_word_start)
    }

    fn qualifier_before_word_in_text(text: &str, rel_word_start: usize) -> Option<String> {
        if rel_word_start == 0 {
            return None;
        }
        let bytes = text.as_bytes();

        // IntelliSense qualifier must be strict `qualifier.<cursor>` form.
        // Do not allow whitespace around `.` so cases like `e .|` / `e. |`
        // are treated as non-qualified context.
        if bytes.get(rel_word_start.saturating_sub(1)) != Some(&b'.') {
            return None;
        }
        let idx = rel_word_start - 1;

        if idx > 0 && bytes.get(idx - 1) == Some(&b'"') {
            let mut pos = idx as isize - 2;
            loop {
                if pos < 0 {
                    break;
                }
                let pos_usize = pos as usize;
                if bytes[pos_usize] == b'"' {
                    if pos_usize > 0 && bytes[pos_usize - 1] == b'"' {
                        // `""` escape sequence inside quoted identifier: skip the pair.
                        pos -= 2;
                        continue;
                    }
                    let quoted = text.get(pos_usize..idx)?;
                    let qualifier = Self::strip_identifier_quotes(quoted);
                    if qualifier.is_empty() {
                        return None;
                    }
                    return Some(qualifier);
                }
                pos -= 1;
            }
            return None;
        }

        let qualifier_candidate = text.get(..idx)?;
        let mut start_byte = qualifier_candidate.len();
        for (pos, ch) in qualifier_candidate.char_indices().rev() {
            if sql_text::is_identifier_char(ch) {
                start_byte = pos;
                continue;
            }
            break;
        }
        if start_byte == qualifier_candidate.len() {
            return None;
        }
        let qualifier = qualifier_candidate.get(start_byte..)?;
        let qualifier = Self::strip_identifier_quotes(qualifier);
        let starts_with_valid_ident_char = qualifier
            .chars()
            .next()
            .is_some_and(sql_text::is_identifier_start_char);
        if qualifier.is_empty() || !starts_with_valid_ident_char {
            None
        } else {
            Some(qualifier)
        }
    }

    fn try_fast_path_intellisense_filter(
        editor: &TextEditor,
        buffer: &TextBuffer,
        text_shadow: &Arc<Mutex<HighlightShadowState>>,
        intellisense_popup: &Arc<Mutex<IntellisensePopup>>,
        runtime: &Arc<IntellisenseRuntimeState>,
        cursor_pos: i32,
        key: Key,
        typed_char: Option<char>,
    ) -> bool {
        if !intellisense_popup
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_visible()
        {
            return false;
        }

        let Some(range) = runtime.completion_range() else {
            return false;
        };
        let start = range.start();
        let end = range.end();

        let cursor = cursor_pos.max(0) as usize;
        if !Self::is_cursor_within_completion_range(cursor, start, end, key, typed_char) {
            return false;
        }

        if !Self::is_fast_filter_key(key, typed_char) {
            return false;
        }

        // Fast path: keep existing suggestions and just filter by the current in-range prefix.
        // This avoids re-tokenizing/re-analyzing SQL on each extra identifier keystroke.
        let prefix = Self::prefix_in_completion_range(buffer, text_shadow, start, cursor_pos);
        let qualifier = Self::qualifier_before_word(buffer, text_shadow, start);
        if Self::should_hide_fast_path_after_delete(&prefix, qualifier.as_deref(), key) {
            intellisense_popup
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .hide();
            runtime.clear_completion_range();
            return true;
        }
        {
            let mut popup = intellisense_popup
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            popup.filter_visible_suggestions_by_prefix(&prefix);
            if !popup.is_visible() {
                runtime.clear_completion_range();
            } else {
                let (popup_width, popup_height) = popup.popup_dimensions();
                let (popup_x, popup_y) =
                    Self::popup_screen_position(editor, cursor_pos, popup_width, popup_height);
                popup.set_position(popup_x, popup_y);
                runtime.set_completion_range(Some(IntellisenseCompletionRange::new(
                    start,
                    cursor.max(start),
                )));
            }
        }
        true
    }

    fn popup_screen_position(
        editor: &TextEditor,
        cursor_pos: i32,
        popup_width: i32,
        popup_height: i32,
    ) -> (i32, i32) {
        let (cursor_x, cursor_y) = editor.position_to_xy(cursor_pos);
        let (win_x, win_y) = editor
            .window()
            .map(|win| (win.x_root(), win.y_root()))
            .unwrap_or((0, 0));

        let mut popup_x = win_x + cursor_x;
        let mut popup_y = win_y + cursor_y + Self::INTELLISENSE_POPUP_Y_OFFSET;

        if let Some(win) = editor.window() {
            let win_w = win.w();
            let win_h = win.h();
            let max_x = (win_x + win_w - popup_width).max(win_x);
            let max_y = (win_y + win_h - popup_height).max(win_y);
            popup_x = popup_x.clamp(win_x, max_x);
            popup_y = popup_y.clamp(win_y, max_y);
        }

        (popup_x, popup_y)
    }

    fn is_cursor_within_completion_range(
        cursor: usize,
        start: usize,
        end: usize,
        key: Key,
        typed_char: Option<char>,
    ) -> bool {
        if cursor >= start && cursor <= end {
            return true;
        }

        // Allow forward typing past the previous end only for identifier-extension input.
        cursor > end
            && typed_char.is_some_and(sql_text::is_identifier_char)
            && !matches!(key, Key::BackSpace | Key::Delete)
    }

    fn is_fast_filter_key(key: Key, typed_char: Option<char>) -> bool {
        if matches!(key, Key::BackSpace | Key::Delete) {
            return true;
        }
        typed_char.is_some_and(sql_text::is_identifier_char)
    }

    fn should_force_full_analysis(ch: char) -> bool {
        ch == '.'
            || ch.is_whitespace()
            || matches!(
                ch,
                ',' | '(' | ')' | '+' | '-' | '*' | '/' | '%' | '=' | '!' | '<' | '>' | ';' | ':'
            )
    }

    fn has_min_intellisense_prefix(word: &str) -> bool {
        let mut chars = word.chars();
        chars.next().is_some() && chars.next().is_some()
    }

    fn should_hide_fast_path_after_delete(prefix: &str, qualifier: Option<&str>, key: Key) -> bool {
        matches!(key, Key::BackSpace | Key::Delete)
            && qualifier.is_none()
            && !Self::has_min_intellisense_prefix(prefix)
    }

    fn should_ignore_keyup_after_manual_trigger(
        key: Key,
        original_key: Key,
        ctrl_or_cmd: bool,
    ) -> bool {
        ctrl_or_cmd && Self::shortcut_key_for_layout(key, original_key) == Key::from_char(' ')
    }

    fn shortcut_key_for_layout(key: Key, original_key: Key) -> Key {
        if (0..=0x7f).contains(&key.bits()) {
            key
        } else {
            original_key
        }
    }

    fn matches_alpha_shortcut(key: Key, ascii: char) -> bool {
        key == Key::from_char(ascii.to_ascii_lowercase())
            || key == Key::from_char(ascii.to_ascii_uppercase())
    }

    fn should_auto_trigger_intellisense_for_forced_char(
        word: &str,
        qualifier: Option<&str>,
    ) -> bool {
        qualifier.is_some() || Self::has_min_intellisense_prefix(word)
    }

    fn prefix_in_completion_range(
        buffer: &TextBuffer,
        text_shadow: &Arc<Mutex<HighlightShadowState>>,
        start: usize,
        cursor_pos: i32,
    ) -> String {
        let cursor = cursor_pos.max(0) as usize;
        let end = cursor.max(start);
        text_buffer_access::text_range(buffer, Some(text_shadow), start as i32, end as i32)
            .chars()
            .filter(|ch| sql_text::is_identifier_char(*ch))
            .collect()
    }

    fn char_before_cursor(
        buffer: &TextBuffer,
        text_shadow: &Arc<Mutex<HighlightShadowState>>,
        cursor_pos: i32,
    ) -> Option<char> {
        if cursor_pos <= 0 {
            return None;
        }
        let start = (cursor_pos - 4).max(0);
        let text = text_buffer_access::text_range(buffer, Some(text_shadow), start, cursor_pos);
        text.chars().next_back()
    }

    fn non_whitespace_char_before_cursor(
        buffer: &TextBuffer,
        text_shadow: &Arc<Mutex<HighlightShadowState>>,
        cursor_pos: i32,
    ) -> Option<char> {
        if cursor_pos <= 0 {
            return None;
        }
        let start = (cursor_pos - INTELLISENSE_CONTEXT_WINDOW).max(0);
        let text = text_buffer_access::text_range(buffer, Some(text_shadow), start, cursor_pos);
        text.chars().rev().find(|ch| !ch.is_whitespace())
    }

    #[cfg(test)]
    fn non_whitespace_char_before_cursor_in_text(text: &str, cursor_pos: usize) -> Option<char> {
        if text.is_empty() || cursor_pos == 0 {
            return None;
        }
        let cursor_pos = cursor_pos.min(text.len());
        let text = text.get(..cursor_pos).unwrap_or("");
        text.chars().rev().find(|ch| !ch.is_whitespace())
    }

    fn typed_char_from_key_event(
        event_text: &str,
        key: Key,
        shift: bool,
        char_before_cursor: Option<char>,
    ) -> Option<char> {
        if let Some(ch) = event_text.chars().next() {
            return Some(ch);
        }

        if key == Key::from_char('-') {
            // FLTK can report '_' as key '-' with empty event_text when Shift state is
            // already released in KeyUp. Infer from the actual inserted buffer character.
            if let Some(prev) = char_before_cursor {
                if prev == '_' || prev == '-' {
                    return Some(prev);
                }
            }
            if shift {
                return Some('_');
            }
            return Some('-');
        }

        None
    }

    fn is_modifier_key(key: Key) -> bool {
        matches!(
            key,
            Key::ShiftL
                | Key::ShiftR
                | Key::ControlL
                | Key::ControlR
                | Key::AltL
                | Key::AltR
                | Key::MetaL
                | Key::MetaR
                | Key::CapsLock
        )
    }
}
