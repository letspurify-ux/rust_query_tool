#[derive(Clone)]
struct HighlightRequest {
    revision: u64,
    generation: u64,
    kind: HighlightRequestKind,
}

#[derive(Clone)]
enum HighlightRequestKind {
    FullText {
        text: String,
    },
    Windowed {
        text_len: usize,
        windows: Vec<WindowHighlightRequest>,
    },
    Incremental {
        text_len: usize,
        request: IncrementalHighlightRequest,
    },
}

#[derive(Clone)]
struct HighlightResult {
    revision: u64,
    generation: u64,
    kind: HighlightResultKind,
}

#[derive(Clone)]
enum HighlightResultKind {
    FullText {
        style_text: String,
    },
    Windowed {
        text_len: usize,
        windows: Vec<WindowHighlightResult>,
    },
    Incremental {
        text_len: usize,
        result: IncrementalHighlightResult,
    },
}

#[derive(Clone)]
struct HighlightWorkerTask {
    editor_id: u64,
    request: HighlightRequest,
    highlighter: Arc<Mutex<SqlHighlighter>>,
    result_sender: mpsc::Sender<HighlightResult>,
}

impl SqlEditorWidget {
    fn highlight_worker_sender_state() -> &'static Mutex<Option<mpsc::Sender<HighlightWorkerTask>>>
    {
        static WORKER_SENDER: OnceLock<Mutex<Option<mpsc::Sender<HighlightWorkerTask>>>> =
            OnceLock::new();
        WORKER_SENDER.get_or_init(|| Mutex::new(None))
    }

    fn highlight_worker_sender() -> Option<mpsc::Sender<HighlightWorkerTask>> {
        let sender_state = Self::highlight_worker_sender_state();
        let mut sender_guard = sender_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if sender_guard.is_none() {
            *sender_guard = Self::spawn_highlight_worker_sender();
        }

        sender_guard.as_ref().cloned()
    }

    fn clear_cached_highlight_worker_sender() {
        let sender_state = Self::highlight_worker_sender_state();
        let mut sender_guard = sender_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        sender_guard.take();
    }

    fn spawn_highlight_worker_sender() -> Option<mpsc::Sender<HighlightWorkerTask>> {
        let (sender, receiver) = mpsc::channel::<HighlightWorkerTask>();
        let spawn_result = thread::Builder::new()
            .name("sql-highlighter-worker-global".to_string())
            .spawn(move || {
                while let Ok(first_task) = receiver.recv() {
                    let mut latest_tasks: HashMap<u64, HighlightWorkerTask> = HashMap::new();
                    latest_tasks.insert(first_task.editor_id, first_task);

                    loop {
                        match receiver.try_recv() {
                            Ok(task) => {
                                latest_tasks.insert(task.editor_id, task);
                            }
                            Err(mpsc::TryRecvError::Empty) => break,
                            Err(mpsc::TryRecvError::Disconnected) => break,
                        }
                    }

                    for task in latest_tasks.into_values() {
                        let request = task.request;
                        let revision = request.revision;
                        let generation = request.generation;
                        let fallback_request_kind = request.kind.clone();
                        let result = panic::catch_unwind(AssertUnwindSafe(move || {
                            let guard = task
                                .highlighter
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            match request.kind {
                                HighlightRequestKind::FullText { text } => {
                                    HighlightResultKind::FullText {
                                        style_text: guard.generate_styles_for_text(&text),
                                    }
                                }
                                HighlightRequestKind::Windowed { text_len, windows } => {
                                    HighlightResultKind::Windowed {
                                        text_len,
                                        windows: guard.generate_window_styles(windows),
                                    }
                                }
                                HighlightRequestKind::Incremental { text_len, request } => {
                                    let result = guard.generate_incremental_styles(request).unwrap_or(
                                        IncrementalHighlightResult {
                                            start: 0,
                                            end: 0,
                                            styles: String::new(),
                                        },
                                    );
                                    HighlightResultKind::Incremental { text_len, result }
                                }
                            }
                        }));

                        match result {
                            Ok(kind) => {
                                if task
                                    .result_sender
                                    .send(HighlightResult {
                                        revision,
                                        generation,
                                        kind,
                                    })
                                    .is_ok()
                                {
                                    app::awake();
                                }
                            }
                            Err(payload) => {
                                let panic_msg =
                                    SqlEditorWidget::panic_payload_to_string(payload.as_ref());
                                crate::utils::logging::log_error(
                                    "sql_editor::highlight_worker",
                                    &format!(
                                        "highlight worker panicked for editor {}: {}",
                                        task.editor_id, panic_msg
                                    ),
                                );

                                let fallback_kind = SqlEditorWidget::fallback_highlight_result_kind(
                                    &fallback_request_kind,
                                );
                                if task
                                    .result_sender
                                    .send(HighlightResult {
                                        revision,
                                        generation,
                                        kind: fallback_kind,
                                    })
                                    .is_ok()
                                {
                                    app::awake();
                                }
                            }
                        }
                    }
                }
            });

        match spawn_result {
            Ok(_) => Some(sender),
            Err(err) => {
                crate::utils::logging::log_error(
                    "sql_editor::highlight_worker",
                    &format!("failed to spawn global highlight worker: {}", err),
                );
                None
            }
        }
    }

    fn next_highlight_editor_id() -> u64 {
        static EDITOR_ID: AtomicU64 = AtomicU64::new(0);
        EDITOR_ID.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn default_style_text_for_len(len: usize) -> String {
        std::iter::repeat_n(STYLE_DEFAULT, len).collect()
    }

    fn text_len_to_i32(text_len: usize) -> Option<i32> {
        i32::try_from(text_len).ok()
    }

    fn ensure_style_buffer_len(style_buffer: &mut TextBuffer, text_len: usize) -> bool {
        let Some(expected_len) = Self::text_len_to_i32(text_len) else {
            crate::utils::logging::log_error(
                "sql_editor::highlight_worker",
                "text too large to represent in FLTK TextBuffer length",
            );
            return false;
        };

        if style_buffer.length() != expected_len {
            style_buffer.set_text(&Self::default_style_text_for_len(text_len));
        }
        true
    }

    fn fallback_highlight_result_kind(request_kind: &HighlightRequestKind) -> HighlightResultKind {
        match request_kind {
            HighlightRequestKind::FullText { text } => HighlightResultKind::FullText {
                style_text: Self::default_style_text_for_len(text.len()),
            },
            HighlightRequestKind::Windowed { text_len, windows } => {
                let mut fallback_windows = Vec::with_capacity(windows.len());
                for window in windows {
                    let start = window.start.min(*text_len);
                    let end = window.end.min(*text_len).max(start);
                    fallback_windows.push(WindowHighlightResult {
                        start,
                        end,
                        styles: Self::default_style_text_for_len(end.saturating_sub(start)),
                    });
                }
                HighlightResultKind::Windowed {
                    text_len: *text_len,
                    windows: fallback_windows,
                }
            }
            HighlightRequestKind::Incremental { text_len, request } => {
                let start = request.start.min(*text_len);
                HighlightResultKind::Incremental {
                    text_len: *text_len,
                    result: IncrementalHighlightResult {
                        start,
                        end: start,
                        styles: String::new(),
                    },
                }
            }
        }
    }
}

impl SqlEditorWidget {
    fn setup_highlight_worker(&self, highlight_result_receiver: mpsc::Receiver<HighlightResult>) {
        let receiver: Arc<Mutex<mpsc::Receiver<HighlightResult>>> =
            Arc::new(Mutex::new(highlight_result_receiver));
        let widget = self.clone();

        fn schedule_poll(
            receiver: Arc<Mutex<mpsc::Receiver<HighlightResult>>>,
            widget: SqlEditorWidget,
        ) {
            if widget.group.was_deleted() {
                return;
            }

            let mut disconnected = false;
            let mut latest_result: Option<HighlightResult> = None;
            {
                let r = receiver
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                loop {
                    match r.try_recv() {
                        Ok(result) => latest_result = Some(result),
                        Err(mpsc::TryRecvError::Empty) => break,
                        Err(mpsc::TryRecvError::Disconnected) => {
                            disconnected = true;
                            break;
                        }
                    }
                }
            }

            if disconnected {
                return;
            }

            if let Some(result) = latest_result {
                let current_revision = widget.highlight_revision.load(Ordering::Relaxed);
                let current_generation = widget.highlight_generation.load(Ordering::Relaxed);
                if result.revision == current_revision && result.generation == current_generation {
                    let mut style_buffer = widget.style_buffer.clone();
                    match result.kind {
                        HighlightResultKind::FullText { style_text } => {
                            style_buffer.set_text(&style_text);
                        }
                        HighlightResultKind::Windowed { text_len, windows } => {
                            if SqlEditorWidget::ensure_style_buffer_len(&mut style_buffer, text_len)
                            {
                                for window in windows {
                                    if window.start >= window.end || window.end > text_len {
                                        continue;
                                    }
                                    if window.styles.len()
                                        != window.end.saturating_sub(window.start)
                                    {
                                        continue;
                                    }
                                    let (Ok(start), Ok(end)) =
                                        (i32::try_from(window.start), i32::try_from(window.end))
                                    else {
                                        continue;
                                    };
                                    if end > start {
                                        style_buffer.replace(start, end, &window.styles);
                                    }
                                }
                            }
                        }
                        HighlightResultKind::Incremental { text_len, result } => {
                            if SqlEditorWidget::ensure_style_buffer_len(&mut style_buffer, text_len)
                            {
                                if result.end <= text_len
                                    && result.start <= result.end
                                    && result.styles.len()
                                        == result.end.saturating_sub(result.start)
                                {
                                    if let (Ok(start), Ok(end)) =
                                        (i32::try_from(result.start), i32::try_from(result.end))
                                    {
                                        if end > start {
                                            style_buffer.replace(start, end, &result.styles);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    let mut editor = widget.editor.clone();
                    editor.redraw();
                    app::redraw();
                }
            }

            app::add_timeout3(PROGRESS_POLL_INTERVAL_SECONDS, move |_| {
                schedule_poll(receiver.clone(), widget.clone());
            });
        }

        schedule_poll(receiver, widget);
    }

    fn setup_viewport_highlight_poll(&self) {
        let widget = self.clone();
        let editor = self.editor.clone();
        let buffer = self.buffer.clone();
        let last_viewport_state = Arc::new(Mutex::new(None::<(bool, i32, i32, i32, i32)>));

        fn schedule_poll(
            widget: SqlEditorWidget,
            editor: TextEditor,
            buffer: TextBuffer,
            last_viewport_state: Arc<Mutex<Option<(bool, i32, i32, i32, i32)>>>,
        ) {
            if widget.group.was_deleted() || editor.was_deleted() {
                return;
            }

            let visible = editor.visible_r();
            let top_row = editor.scroll_row();
            let left_col = editor.scroll_col();
            let w = editor.w();
            let h = editor.h();
            let text_len = buffer.length();
            let current_state = (visible, top_row, left_col, w, h);
            let should_refresh = {
                let mut previous = last_viewport_state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let changed = previous.is_none_or(|state| state != current_state);
                if changed {
                    *previous = Some(current_state);
                }
                changed
            };

            if should_refresh
                && SqlEditorWidget::should_use_windowed_highlighting(text_len.max(0) as usize)
            {
                widget.refresh_highlighting();
            }

            app::add_timeout3(VIEWPORT_HIGHLIGHT_POLL_INTERVAL_SECONDS, move |_| {
                schedule_poll(
                    widget.clone(),
                    editor.clone(),
                    buffer.clone(),
                    last_viewport_state.clone(),
                );
            });
        }

        schedule_poll(widget, editor, buffer, last_viewport_state);
    }

    #[allow(dead_code)]
    pub fn update_highlight_data(&mut self, data: HighlightData) {
        self.highlighter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .set_highlight_data(data);
        self.highlight_generation.fetch_add(1, Ordering::Relaxed);
        self.refresh_highlighting();
    }

    pub fn get_highlighter(&self) -> Arc<Mutex<SqlHighlighter>> {
        self.highlighter.clone()
    }

    fn should_use_windowed_highlighting(text_len: usize) -> bool {
        text_len > WINDOWED_HIGHLIGHT_THRESHOLD
    }

    fn invalidate_pending_highlight_results(&self) {
        self.highlight_revision.fetch_add(1, Ordering::Relaxed);
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
        if !Self::should_use_windowed_highlighting(text_len) {
            if ins > 0 || del > 0 {
                let mut style_buffer = self.style_buffer.clone();
                Self::apply_style_buffer_edit_delta(&mut style_buffer, pos, ins, del);
                if !Self::ensure_style_buffer_len(&mut style_buffer, text_len) {
                    return;
                }
            }
            self.enqueue_highlight_request(buf.text());
            return;
        }

        self.invalidate_pending_highlight_results();

        let mut style_buffer = self.style_buffer.clone();
        Self::apply_style_buffer_edit_delta(&mut style_buffer, pos, ins, del);
        if !Self::ensure_style_buffer_len(&mut style_buffer, text_len) {
            return;
        }

        let text = buf.text();
        let previous_styles = style_buffer.text();
        let start = compute_incremental_start_from_text(&text, pos, ins, del);
        let start = start.min(text_len);
        let entry_state = {
            let highlighter = self
                .highlighter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            highlighter.probe_entry_state_for_style_text(&text, &previous_styles, start)
        };

        self.enqueue_incremental_highlight_request(
            text_len,
            IncrementalHighlightRequest {
                start,
                text,
                previous_styles,
                entry_state,
            },
        );

        if needs_full_rehighlight(buf, pos, ins, deleted_text) {
            let cursor_pos = infer_cursor_after_edit(pos, ins, text_len);
            self.apply_windowed_highlighting(cursor_pos, None, None, None);
        }
    }

    fn apply_windowed_highlighting(
        &self,
        cursor_pos: usize,
        edited_range: Option<(usize, usize)>,
        viewport: Option<(usize, usize)>,
        edit_delta: Option<(i32, i32, i32)>,
    ) {
        let text_len = self.buffer.length().max(0) as usize;
        let cursor = cursor_pos.min(text_len);
        let edited_range =
            edited_range.map(|(start, end)| normalize_highlight_range(start, end, text_len));
        let viewport = viewport.map(|(start, end)| normalize_highlight_range(start, end, text_len));

        let mut style_buffer = self.style_buffer.clone();
        if let Some((pos, ins, del)) = edit_delta {
            Self::apply_style_buffer_edit_delta(&mut style_buffer, pos, ins, del);
        }
        if !Self::ensure_style_buffer_len(&mut style_buffer, text_len) {
            return;
        }

        let window_requests = {
            let highlighter = self
                .highlighter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            highlighter.prepare_window_highlight_requests(
                &self.buffer,
                &style_buffer,
                cursor,
                edited_range,
                viewport,
            )
        };

        let mut editor = self.editor.clone();
        editor.redraw();
        app::redraw();

        if window_requests.is_empty() {
            return;
        }
        self.enqueue_windowed_highlight_request(text_len, window_requests);
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

    fn enqueue_worker_task_with_retry(&self, task: HighlightWorkerTask) {
        let revision = task.request.revision;
        let generation = task.request.generation;
        let fallback_kind = task.request.kind.clone();
        let Some(sender) = Self::highlight_worker_sender() else {
            crate::utils::logging::log_error(
                "sql_editor::highlight_worker",
                "highlight worker unavailable; failed to enqueue request",
            );
            self.enqueue_fallback_highlight_result(revision, generation, &fallback_kind);
            return;
        };

        if sender.send(task.clone()).is_ok() {
            return;
        }

        Self::clear_cached_highlight_worker_sender();
        crate::utils::logging::log_error(
            "sql_editor::highlight_worker",
            "failed to enqueue highlight request; retrying with a new worker",
        );

        if let Some(retry_sender) = Self::highlight_worker_sender() {
            if let Err(err) = retry_sender.send(task) {
                Self::clear_cached_highlight_worker_sender();
                crate::utils::logging::log_error(
                    "sql_editor::highlight_worker",
                    &format!("failed to enqueue highlight request after retry: {}", err),
                );
                self.enqueue_fallback_highlight_result(revision, generation, &fallback_kind);
            }
        } else {
            Self::clear_cached_highlight_worker_sender();
            crate::utils::logging::log_error(
                "sql_editor::highlight_worker",
                "highlight worker unavailable after retry; request dropped",
            );
            self.enqueue_fallback_highlight_result(revision, generation, &fallback_kind);
        }
    }

    fn enqueue_fallback_highlight_result(
        &self,
        revision: u64,
        generation: u64,
        request_kind: &HighlightRequestKind,
    ) {
        let fallback = HighlightResult {
            revision,
            generation,
            kind: Self::fallback_highlight_result_kind(request_kind),
        };
        if self.highlight_result_sender.send(fallback).is_ok() {
            app::awake();
        }
    }

    fn enqueue_highlight_request(&self, text: String) {
        let revision = self.highlight_revision.fetch_add(1, Ordering::Relaxed) + 1;
        let generation = self.highlight_generation.load(Ordering::Relaxed);
        let request_kind = HighlightRequestKind::FullText { text };
        let task = HighlightWorkerTask {
            editor_id: self.highlight_editor_id,
            request: HighlightRequest {
                revision,
                generation,
                kind: request_kind,
            },
            highlighter: self.highlighter.clone(),
            result_sender: self.highlight_result_sender.clone(),
        };

        self.enqueue_worker_task_with_retry(task);
    }

    fn enqueue_incremental_highlight_request(
        &self,
        text_len: usize,
        request: IncrementalHighlightRequest,
    ) {
        let revision = self.highlight_revision.load(Ordering::Relaxed);
        let generation = self.highlight_generation.load(Ordering::Relaxed);
        let task = HighlightWorkerTask {
            editor_id: self.highlight_editor_id,
            request: HighlightRequest {
                revision,
                generation,
                kind: HighlightRequestKind::Incremental { text_len, request },
            },
            highlighter: self.highlighter.clone(),
            result_sender: self.highlight_result_sender.clone(),
        };

        self.enqueue_worker_task_with_retry(task);
    }

    fn enqueue_windowed_highlight_request(
        &self,
        text_len: usize,
        window_requests: Vec<WindowHighlightRequest>,
    ) {
        if window_requests.is_empty() {
            return;
        }

        let revision = self.highlight_revision.load(Ordering::Relaxed);
        let generation = self.highlight_generation.load(Ordering::Relaxed);
        let task = HighlightWorkerTask {
            editor_id: self.highlight_editor_id,
            request: HighlightRequest {
                revision,
                generation,
                kind: HighlightRequestKind::Windowed {
                    text_len,
                    windows: window_requests,
                },
            },
            highlighter: self.highlighter.clone(),
            result_sender: self.highlight_result_sender.clone(),
        };

        self.enqueue_worker_task_with_retry(task);
    }

    #[allow(dead_code)]
    pub fn refresh_highlighting(&self) {
        let text_len = self.buffer.length().max(0) as usize;
        if Self::should_use_windowed_highlighting(text_len) {
            self.invalidate_pending_highlight_results();
            let cursor_pos = self
                .editor
                .insert_position()
                .clamp(0, self.buffer.length())
                .max(0) as usize;
            let viewport = editor_viewport_range(&self.editor, &self.buffer);
            self.apply_windowed_highlighting(cursor_pos, None, viewport, None);
            return;
        }

        self.enqueue_highlight_request(self.buffer.text());
    }
}

fn collect_highlight_columns_from_intellisense(data: &IntellisenseData) -> Vec<String> {
    data.get_all_columns_for_highlighting()
}

fn normalize_highlight_range(start: usize, end: usize, text_len: usize) -> (usize, usize) {
    let mut bounded_start = start.min(text_len);
    let mut bounded_end = end.min(text_len);
    if bounded_start > bounded_end {
        std::mem::swap(&mut bounded_start, &mut bounded_end);
    }
    (bounded_start, bounded_end)
}

fn editor_viewport_range(editor: &TextEditor, buffer: &TextBuffer) -> Option<(usize, usize)> {
    let text_len = buffer.length().max(0) as usize;
    if text_len == 0 {
        return Some((0, 0));
    }

    if !editor.visible_r() {
        return None;
    }

    let mut editor = editor.clone();
    let h = editor.h();
    let text_size = editor.text_size().max(1);
    if h <= 0 {
        return None;
    }

    // FLTK scroll_row is 1-based in practice; normalize to 0-based line count.
    let top_row = editor.scroll_row().max(1).saturating_sub(1);
    let start_pos = editor
        .skip_lines(0, top_row, true)
        .clamp(0, buffer.length());
    let visible_rows = (h / text_size).max(1).saturating_add(2);
    let end_pos = editor
        .skip_lines(start_pos, visible_rows, true)
        .clamp(start_pos, buffer.length());

    let line_start = buffer.line_start(start_pos).max(0) as usize;
    let line_end = buffer.line_end(end_pos).max(0) as usize;
    let start = line_start.min(text_len);
    let end = line_end.min(text_len).max(start);
    Some((start, end))
}

fn infer_cursor_after_edit(pos: i32, ins: i32, text_len: usize) -> usize {
    let base = pos.max(0) as usize;
    let inserted = ins.max(0) as usize;
    base.saturating_add(inserted).min(text_len)
}

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

#[cfg(test)]
fn compute_edited_range(pos: i32, ins: i32, del: i32, text_len: usize) -> Option<(usize, usize)> {
    if pos < 0 {
        return None;
    }

    let start = (pos as usize).min(text_len);
    let inserted = ins.max(0) as usize;
    let deleted = del.max(0) as usize;
    let changed_len = inserted.max(deleted);
    let end = start.saturating_add(changed_len).min(text_len);

    Some((start, end))
}


fn needs_full_rehighlight(buf: &TextBuffer, pos: i32, ins: i32, deleted_text: &str) -> bool {
    if !deleted_text.is_empty() {
        if deleted_text.len() > DIRECT_STATEFUL_DELIMITER_SCAN_LIMIT {
            return true;
        }
        if has_stateful_sql_delimiter(deleted_text) {
            return true;
        }
    }

    if ins > 0 {
        if pos < 0 {
            return true;
        }
        let insert_len = ins.max(0) as usize;
        if insert_len > DIRECT_STATEFUL_DELIMITER_SCAN_LIMIT {
            return true;
        }

        let insert_end = pos.saturating_add(ins).min(buf.length());
        if let Some(inserted_text) = buf.text_range(pos, insert_end) {
            if has_stateful_sql_delimiter(&inserted_text) {
                return true;
            }
        } else {
            return true;
        }
    }

    if pos < 0 {
        return false;
    }

    let sample_start = pos.saturating_sub(2);
    let sample_end = pos
        .saturating_add(ins.max(0))
        .saturating_add(2)
        .min(buf.length());
    let nearby = buf.text_range(sample_start, sample_end).unwrap_or_default();
    has_stateful_sql_delimiter(&nearby)
}

fn has_stateful_sql_delimiter(text: &str) -> bool {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum DelimiterScanState {
        Normal,
        SawSlash,
        SawDash,
        SawStar,
    }

    let mut state = DelimiterScanState::Normal;

    for byte in text.bytes() {
        if byte == b'\'' {
            return true;
        }

        state = match state {
            DelimiterScanState::Normal => match byte {
                b'/' => DelimiterScanState::SawSlash,
                b'-' => DelimiterScanState::SawDash,
                b'*' => DelimiterScanState::SawStar,
                _ => DelimiterScanState::Normal,
            },
            DelimiterScanState::SawSlash => match byte {
                b'*' => return true,
                b'/' => DelimiterScanState::SawSlash,
                b'-' => DelimiterScanState::SawDash,
                _ => DelimiterScanState::Normal,
            },
            DelimiterScanState::SawDash => match byte {
                b'-' => return true,
                b'/' => DelimiterScanState::SawSlash,
                b'*' => DelimiterScanState::SawStar,
                _ => DelimiterScanState::Normal,
            },
            DelimiterScanState::SawStar => match byte {
                b'/' => return true,
                b'-' => DelimiterScanState::SawDash,
                b'*' => DelimiterScanState::SawStar,
                _ => DelimiterScanState::Normal,
            },
        };
    }

    false
}

#[allow(dead_code)]
fn style_before(style_buffer: &TextBuffer, pos: i32) -> Option<char> {
    if pos <= 0 {
        return None;
    }

    let end = pos.min(style_buffer.length());
    let start = end.saturating_sub(1);
    style_buffer
        .text_range(start, end)
        .and_then(|text| text.chars().next())
}

#[allow(dead_code)]
fn is_string_or_comment_style(style: char) -> bool {
    style == STYLE_COMMENT || style == STYLE_STRING
}
