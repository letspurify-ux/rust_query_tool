#[derive(Clone, Default)]
pub(crate) struct HighlightShadowState {
    text: String,
    styles: Vec<u8>,
    newline_positions: Vec<usize>,
}

impl HighlightShadowState {
    pub(crate) fn rebuild(&mut self, text: String, styles: &str) {
        self.text = text;
        self.styles = styles.as_bytes().to_vec();
        self.rebuild_newline_positions();
    }

    pub(crate) fn clear(&mut self) {
        self.text.clear();
        self.styles.clear();
        self.newline_positions.clear();
    }

    fn rebuild_newline_positions(&mut self) {
        self.newline_positions.clear();
        extend_line_break_positions(&mut self.newline_positions, &self.text, 0);
    }

    pub(crate) fn len(&self) -> usize {
        self.text.len()
    }

    fn lower_bound(&self, target: usize) -> usize {
        self.newline_positions.partition_point(|&pos| pos < target)
    }

    pub(crate) fn line_start(&self, pos: usize) -> usize {
        if self.text.is_empty() {
            return 0;
        }

        let pos = pos.min(self.text.len());
        let idx = self.lower_bound(pos);
        if idx == 0 {
            0
        } else {
            self.newline_positions[idx - 1].saturating_add(1)
        }
    }

    fn inclusive_line_end(&self, pos: usize) -> usize {
        let text_len = self.text.len();
        if text_len == 0 {
            return 0;
        }

        let pos = pos.min(text_len);
        let idx = self.lower_bound(pos);
        self.newline_positions
            .get(idx)
            .copied()
            .map(|line_end| line_end.saturating_add(1).min(text_len))
            .unwrap_or(text_len)
    }

    pub(crate) fn line_end(&self, pos: usize) -> usize {
        let text_len = self.text.len();
        if text_len == 0 {
            return 0;
        }

        let pos = pos.min(text_len);
        let idx = self.lower_bound(pos);
        self.newline_positions.get(idx).copied().unwrap_or(text_len)
    }

    fn continuation_style_before_position(&self, pos: usize) -> char {
        if pos == 0 {
            return STYLE_DEFAULT;
        }

        let style = self
            .styles
            .get(pos.saturating_sub(1))
            .copied()
            .map(char::from)
            .unwrap_or(STYLE_DEFAULT);
        if is_continuation_style(style) {
            style
        } else {
            STYLE_DEFAULT
        }
    }
    pub(crate) fn text_range_string(&self, start: usize, end: usize) -> Option<String> {
        let start = Self::clamp_boundary(&self.text, start.min(self.text.len()));
        let end = Self::clamp_boundary(&self.text, end.min(self.text.len()));
        if end < start {
            return Some(String::new());
        }
        self.text.get(start..end).map(ToString::to_string)
    }

    fn style_slice(&self, start: usize, end: usize) -> Option<&str> {
        self.styles
            .get(start..end)
            .and_then(|slice| std::str::from_utf8(slice).ok())
    }

    fn clamp_boundary(text: &str, pos: usize) -> usize {
        let mut clamped = pos.min(text.len());
        while clamped > 0 && !text.is_char_boundary(clamped) {
            clamped -= 1;
        }
        clamped
    }

    fn shift_offset(pos: usize, delta: isize) -> usize {
        if delta >= 0 {
            pos.saturating_add(delta as usize)
        } else {
            pos.saturating_sub(delta.unsigned_abs())
        }
    }

    fn apply_edit(&mut self, pos: usize, inserted_text: &str, deleted_len: usize) -> bool {
        let start = Self::clamp_boundary(&self.text, pos);
        let end = Self::clamp_boundary(&self.text, start.saturating_add(deleted_len));
        if end < start {
            return false;
        }

        let start_newline_idx = self.lower_bound(start);
        let end_newline_idx = self.lower_bound(end);
        let mut trailing_newlines = self.newline_positions.split_off(end_newline_idx);
        self.newline_positions.truncate(start_newline_idx);

        let replaced_len = end.saturating_sub(start);
        let delta = inserted_text.len() as isize - replaced_len as isize;
        for pos in &mut trailing_newlines {
            *pos = Self::shift_offset(*pos, delta);
        }
        self.newline_positions.extend(
            line_break_positions_with_offset(inserted_text, start),
        );
        self.newline_positions.extend(trailing_newlines);

        if self.text.get(start..end).is_none() {
            return false;
        }
        self.text.replace_range(start..end, inserted_text);
        self.styles
            .splice(start..end, std::iter::repeat_n(STYLE_DEFAULT as u8, inserted_text.len()));
        true
    }
}

fn line_break_positions_with_offset(text: &str, offset: usize) -> impl Iterator<Item = usize> + '_ {
    let bytes = text.as_bytes();
    let mut idx = 0usize;
    std::iter::from_fn(move || {
        while idx < bytes.len() {
            let current = idx;
            idx += 1;
            match bytes.get(current).copied() {
                Some(b'\n') => return Some(offset.saturating_add(current)),
                Some(b'\r') => {
                    if bytes.get(idx) == Some(&b'\n') {
                        idx += 1;
                        return Some(offset.saturating_add(current.saturating_add(1)));
                    }
                    return Some(offset.saturating_add(current));
                }
                _ => {}
            }
        }
        None
    })
}

fn extend_line_break_positions(target: &mut Vec<usize>, text: &str, offset: usize) {
    target.extend(line_break_positions_with_offset(text, offset));
}

impl SqlEditorWidget {
    fn default_style_text_for_len(len: usize) -> String {
        std::iter::repeat_n(STYLE_DEFAULT, len).collect()
    }
}

impl SqlEditorWidget {
    pub fn update_highlight_data(&mut self, data: HighlightData) {
        self.highlighter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .set_highlight_data(data);
        self.rehighlight_full_buffer();
    }

    pub fn get_highlighter(&self) -> Arc<Mutex<SqlHighlighter>> {
        self.highlighter.clone()
    }

    fn handle_buffer_highlight_update(
        &self,
        buf: &TextBuffer,
        pos: i32,
        ins: i32,
        del: i32,
        deleted_text: &str,
    ) {
        let text_len = buf.length().max(0) as usize;
        let inserted_text = inserted_text(buf, &self.highlight_shadow, pos, ins);
        let expected_previous_len = text_len
            .saturating_add(del.max(0) as usize)
            .saturating_sub(ins.max(0) as usize);
        let mut style_buffer = self.style_buffer.clone();
        Self::apply_style_buffer_edit_delta(&mut style_buffer, pos, ins, del);
        if style_buffer.length().max(0) as usize != text_len {
            self.rehighlight_full_buffer();
            return;
        }
        if text_len == 0 {
            self.highlight_shadow
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clear();
            let mut editor = self.editor.clone();
            editor.redraw();
            return;
        }

        let updated = {
            let mut shadow = self
                .highlight_shadow
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if shadow.len() != expected_previous_len || shadow.styles.len() != expected_previous_len {
                drop(shadow);
                self.rehighlight_full_buffer();
                return;
            }

            let shadow_pos = pos.max(0) as usize;
            if !shadow.apply_edit(shadow_pos, &inserted_text, del.max(0) as usize) {
                drop(shadow);
                self.rehighlight_full_buffer();
                return;
            }

            self.apply_main_thread_incremental_highlighting(
                &mut shadow,
                &mut style_buffer,
                shadow_pos,
                ins.max(0) as usize,
                del.max(0) as usize,
                &inserted_text,
                deleted_text,
            )
        };
        if updated {
            let mut editor = self.editor.clone();
            editor.redraw();
        }
    }

    fn apply_style_buffer_edit_delta(style_buffer: &mut TextBuffer, pos: i32, ins: i32, del: i32) {
        if ins <= 0 && del <= 0 {
            return;
        }

        let style_len = style_buffer.length().max(0);
        let start = pos.clamp(0, style_len);
        let delete_len = del.max(0);
        let delete_end = start.saturating_add(delete_len).min(style_len);
        if delete_end > start {
            style_buffer.remove(start, delete_end);
        }

        if ins > 0 {
            let default_styles = Self::default_style_text_for_len(ins as usize);
            style_buffer.replace(start, start, &default_styles);
        }
    }

    fn apply_main_thread_incremental_highlighting(
        &self,
        shadow: &mut HighlightShadowState,
        style_buffer: &mut TextBuffer,
        pos: usize,
        ins: usize,
        del: usize,
        inserted_text: &str,
        deleted_text: &str,
    ) -> bool {
        let text_len = shadow.len();
        if text_len == 0 {
            return false;
        }

        let start = incremental_rehighlight_start(shadow, pos, inserted_text, deleted_text).min(text_len);
        let must_cover_end =
            incremental_direct_rehighlight_end(shadow, pos, ins, del, text_len);
        let highlighter = self
            .highlighter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut current_start = start;
        let mut minimum_end = must_cover_end.max(start);
        let mut entry_state = highlighter.entry_state_from_continuation_style(
            continuation_style_before_position(shadow, start),
        );
        let mut changed_range: Option<(usize, usize)> = None;

        while current_start < text_len {
            let current_end = incremental_line_chunk_end(shadow, current_start, minimum_end, text_len);
            if current_end <= current_start {
                break;
            }

            let Some(range_text) = shadow.text.get(current_start..current_end) else {
                break;
            };
            let Some(previous_styles) = shadow.styles.get(current_start..current_end) else {
                break;
            };

            let old_exit_style = continuation_style_before_position(shadow, current_end);
            let (new_styles, new_exit_state) =
                highlighter.generate_styles_for_window(range_text, entry_state);
            if new_styles.len() != range_text.len() {
                break;
            }

            if new_styles.as_bytes() != previous_styles {
                if let Some(style_slice) = shadow.styles.get_mut(current_start..current_end) {
                    style_slice.copy_from_slice(new_styles.as_bytes());
                    changed_range = Some(match changed_range {
                        Some((start, end)) => (start.min(current_start), end.max(current_end)),
                        None => (current_start, current_end),
                    });
                } else {
                    break;
                }
            }

            if current_end >= must_cover_end
                && continuation_style_for_lexer_state(new_exit_state) == old_exit_style
            {
                break;
            }
            if current_end >= text_len {
                break;
            }

            current_start = current_end;
            minimum_end = current_start.saturating_add(1);
            entry_state = new_exit_state;
        }

        let Some((changed_start, changed_end)) = changed_range else {
            return false;
        };
        let (Ok(start_i32), Ok(end_i32)) = (i32::try_from(changed_start), i32::try_from(changed_end))
        else {
            return false;
        };
        let Some(style_text) = shadow.style_slice(changed_start, changed_end) else {
            return false;
        };
        style_buffer.replace(start_i32, end_i32, style_text);
        true
    }

    fn rehighlight_full_buffer(&self) {
        let text = self.buffer.text();
        let styles = {
            let highlighter = self
                .highlighter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            highlighter.generate_styles_for_text(&text)
        };
        let mut style_buffer = self.style_buffer.clone();
        style_buffer.set_text(&styles);
        self.highlight_shadow
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .rebuild(text, &styles);
        let mut editor = self.editor.clone();
        editor.redraw();
    }
}

fn collect_highlight_columns_from_intellisense(data: &IntellisenseData) -> Vec<String> {
    data.get_all_columns_for_highlighting()
}

fn continuation_style_for_lexer_state(state: crate::ui::syntax_highlight::LexerState) -> char {
    match state {
        crate::ui::syntax_highlight::LexerState::Normal => STYLE_DEFAULT,
        crate::ui::syntax_highlight::LexerState::InBlockComment => {
            crate::ui::syntax_highlight::STYLE_BLOCK_COMMENT
        }
        crate::ui::syntax_highlight::LexerState::InHintComment => {
            crate::ui::syntax_highlight::STYLE_HINT
        }
        crate::ui::syntax_highlight::LexerState::InSingleQuote => STYLE_STRING,
        crate::ui::syntax_highlight::LexerState::InQQuote { .. } => {
            crate::ui::syntax_highlight::STYLE_Q_QUOTE_STRING
        }
        crate::ui::syntax_highlight::LexerState::InDoubleQuote => {
            crate::ui::syntax_highlight::STYLE_QUOTED_IDENTIFIER
        }
    }
}

fn continuation_style_before_position(shadow: &HighlightShadowState, pos: usize) -> char {
    shadow.continuation_style_before_position(pos)
}

fn is_continuation_style(style: char) -> bool {
    matches!(
        style,
        STYLE_COMMENT
            | STYLE_STRING
            | crate::ui::syntax_highlight::STYLE_BLOCK_COMMENT
            | crate::ui::syntax_highlight::STYLE_Q_QUOTE_STRING
            | crate::ui::syntax_highlight::STYLE_IDENTIFIER
            | crate::ui::syntax_highlight::STYLE_QUOTED_IDENTIFIER
            | crate::ui::syntax_highlight::STYLE_HINT
    )
}

fn incremental_rehighlight_start(
    shadow: &HighlightShadowState,
    pos: usize,
    _inserted_text: &str,
    _deleted_text: &str,
) -> usize {
    shadow.line_start(pos)
}

fn incremental_direct_rehighlight_end(
    shadow: &HighlightShadowState,
    pos: usize,
    ins: usize,
    del: usize,
    text_len: usize,
) -> usize {
    if text_len == 0 {
        return 0;
    }

    let start = pos.min(text_len);
    let changed_span = ins.max(del);
    let changed_end = start.saturating_add(changed_span).min(text_len);
    shadow.inclusive_line_end(changed_end)
}

fn incremental_line_chunk_end(
    shadow: &HighlightShadowState,
    start: usize,
    minimum_end: usize,
    text_len: usize,
) -> usize {
    if start >= text_len {
        return text_len;
    }

    let target = minimum_end.max(start.saturating_add(1)).min(text_len);
    if target >= text_len {
        return text_len;
    }
    if target > 0
        && shadow
            .text
            .as_bytes()
            .get(target - 1)
            .copied()
            .is_some_and(|byte| byte == b'\n')
    {
        return target;
    }
    let end = shadow.inclusive_line_end(target);
    end.max(start.saturating_add(1)).min(text_len)
}

#[cfg(test)]
fn compute_incremental_start_from_text(text: &str, pos: i32, ins: i32, del: i32) -> usize {
    if text.is_empty() {
        return 0;
    }

    let text_len = text.len();
    let raw_start = pos.max(0) as usize;
    let start = raw_start.min(text_len);
    let changed_end = start
        .saturating_add((ins.max(0) as usize).max(del.max(0) as usize))
        .min(text_len);
    let mut probe = start.min(changed_end);
    while probe > 0 && !text.is_char_boundary(probe) {
        probe -= 1;
    }

    let bytes = text.as_bytes();
    while probe > 0 {
        let prev = probe - 1;
        let Some(&byte) = bytes.get(prev) else {
            break;
        };
        if byte == b'\n' || byte == b'\r' || byte.is_ascii_whitespace() {
            break;
        }
        probe -= 1;
    }

    while probe > 0 && !text.is_char_boundary(probe) {
        probe -= 1;
    }

    probe
}

#[allow(dead_code)]
fn is_string_or_comment_style(style: char) -> bool {
    is_continuation_style(style)
}
