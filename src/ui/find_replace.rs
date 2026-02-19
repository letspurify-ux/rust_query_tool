use crate::ui::center_on_main;
use crate::ui::constants::*;
use crate::ui::theme;
use fltk::{
    app,
    button::{Button, CheckButton},
    enums::{CallbackTrigger, FrameType},
    group::Flex,
    input::Input,
    prelude::*,
    text::{TextBuffer, TextEditor},
    window::Window,
};
use std::cell::RefCell;
use std::rc::Rc;

/// Find/Replace dialog
pub struct FindReplaceDialog;

#[derive(Clone, Default)]
struct FindReplaceSessionState {
    find_text: String,
    replace_text: String,
    case_sensitive: bool,
    search_pos: i32,
    last_search_text: String,
}

thread_local! {
    static FIND_REPLACE_SESSION: RefCell<FindReplaceSessionState> =
        RefCell::new(FindReplaceSessionState::default());
}

fn normalize_search_pos(text: &str, pos: i32) -> i32 {
    if text.is_empty() {
        return 0;
    }
    let mut p = (pos.max(0) as usize).min(text.len());
    if text.is_char_boundary(p) {
        return p as i32;
    }

    // Clamp invalid UTF-8 byte offsets to the previous valid boundary.
    while p > 0 && !text.is_char_boundary(p) {
        p -= 1;
    }
    p as i32
}

fn save_find_replace_state(
    find_input: &Input,
    replace_input: Option<&Input>,
    case_check: &CheckButton,
    search_pos: i32,
    last_search_text: &str,
) {
    FIND_REPLACE_SESSION.with(|state| {
        let mut state = state.borrow_mut();
        state.find_text = find_input.value();
        if let Some(replace_input) = replace_input {
            state.replace_text = replace_input.value();
        }
        state.case_sensitive = case_check.value();
        state.search_pos = search_pos.max(0);
        state.last_search_text = last_search_text.to_string();
    });
}

impl FindReplaceDialog {
    pub fn has_search_text() -> bool {
        FIND_REPLACE_SESSION.with(|state| !state.borrow().find_text.is_empty())
    }

    pub fn show_find_with_registry(
        editor: &mut TextEditor,
        buffer: &mut TextBuffer,
        popups: Rc<RefCell<Vec<Window>>>,
    ) {
        Self::show_dialog(editor, buffer, false, popups);
    }

    /// Show find and replace dialog
    pub fn show_replace_with_registry(
        editor: &mut TextEditor,
        buffer: &mut TextBuffer,
        popups: Rc<RefCell<Vec<Window>>>,
    ) {
        Self::show_dialog(editor, buffer, true, popups);
    }

    fn show_dialog(
        editor: &mut TextEditor,
        buffer: &mut TextBuffer,
        show_replace: bool,
        popups: Rc<RefCell<Vec<Window>>>,
    ) {
        enum DialogMessage {
            FindNext {
                search_text: String,
                case_sensitive: bool,
            },
            Replace {
                search_text: String,
                replace_text: String,
                case_sensitive: bool,
            },
            ReplaceAll {
                search_text: String,
                replace_text: String,
                case_sensitive: bool,
            },
            Close,
        }

        let title = if show_replace {
            "Find and Replace"
        } else {
            "Find"
        };
        let height = if show_replace { 180 } else { 130 };

        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let mut dialog = Window::default().with_size(450, height).with_label(title);
        center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);

        let mut main_flex = Flex::default().with_pos(10, 10).with_size(430, height - 20);
        main_flex.set_type(fltk::group::FlexType::Column);
        main_flex.set_spacing(DIALOG_SPACING);

        // Find input row
        let mut find_flex = Flex::default();
        find_flex.set_type(fltk::group::FlexType::Row);
        let mut find_label = fltk::frame::Frame::default().with_label("Find:");
        find_label.set_label_color(theme::text_primary());
        find_flex.fixed(&find_label, FORM_LABEL_WIDTH);
        let mut find_input = Input::default();
        find_input.set_color(theme::input_bg());
        find_input.set_text_color(theme::text_primary());
        find_input.set_trigger(CallbackTrigger::EnterKeyAlways);
        find_flex.end();
        main_flex.fixed(&find_flex, INPUT_ROW_HEIGHT);

        // Replace input row (if show_replace)
        let replace_input = if show_replace {
            let mut replace_flex = Flex::default();
            replace_flex.set_type(fltk::group::FlexType::Row);
            let mut replace_label = fltk::frame::Frame::default().with_label("Replace:");
            replace_label.set_label_color(theme::text_primary());
            replace_flex.fixed(&replace_label, FORM_LABEL_WIDTH);
            let mut input = Input::default();
            input.set_color(theme::input_bg());
            input.set_text_color(theme::text_primary());
            replace_flex.end();
            main_flex.fixed(&replace_flex, INPUT_ROW_HEIGHT);
            Some(input)
        } else {
            None
        };

        // Options row
        let mut options_flex = Flex::default();
        options_flex.set_type(fltk::group::FlexType::Row);
        let mut case_check = CheckButton::default().with_label("Case sensitive");
        case_check.set_label_color(theme::text_secondary());
        options_flex.end();
        main_flex.fixed(&options_flex, CHECKBOX_ROW_HEIGHT);

        // Buttons row
        let mut button_flex = Flex::default();
        button_flex.set_type(fltk::group::FlexType::Row);
        button_flex.set_spacing(DIALOG_SPACING);

        let _spacer = fltk::frame::Frame::default();

        let mut find_next_btn = Button::default()
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
            .with_label("Find Next");
        find_next_btn.set_color(theme::button_primary());
        find_next_btn.set_label_color(theme::text_primary());
        find_next_btn.set_frame(FrameType::RFlatBox);

        let replace_btn = if show_replace {
            let mut btn = Button::default()
                .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
                .with_label("Replace");
            btn.set_color(theme::button_secondary());
            btn.set_label_color(theme::text_primary());
            btn.set_frame(FrameType::RFlatBox);
            button_flex.fixed(&btn, BUTTON_WIDTH);
            Some(btn)
        } else {
            None
        };

        let replace_all_btn = if show_replace {
            let mut btn = Button::default()
                .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
                .with_label("Replace All");
            btn.set_color(theme::button_secondary());
            btn.set_label_color(theme::text_primary());
            btn.set_frame(FrameType::RFlatBox);
            button_flex.fixed(&btn, BUTTON_WIDTH);
            Some(btn)
        } else {
            None
        };

        let mut close_btn = Button::default()
            .with_size(BUTTON_WIDTH_SMALL, BUTTON_HEIGHT)
            .with_label("Close");
        close_btn.set_color(theme::button_subtle());
        close_btn.set_label_color(theme::text_primary());
        close_btn.set_frame(FrameType::RFlatBox);

        button_flex.fixed(&find_next_btn, BUTTON_WIDTH);
        button_flex.fixed(&close_btn, BUTTON_WIDTH_SMALL);
        button_flex.end();
        main_flex.fixed(&button_flex, BUTTON_ROW_HEIGHT);

        main_flex.end();
        dialog.end();
        fltk::group::Group::set_current(current_group.as_ref());

        popups.borrow_mut().push(dialog.clone());
        let session_snapshot = FIND_REPLACE_SESSION.with(|state| state.borrow().clone());

        if !session_snapshot.find_text.is_empty() {
            find_input.set_value(&session_snapshot.find_text);
        }
        case_check.set_value(session_snapshot.case_sensitive);

        if let Some(mut replace_input_widget) = replace_input.clone() {
            if !session_snapshot.replace_text.is_empty() {
                replace_input_widget.set_value(&session_snapshot.replace_text);
            }
        }

        // State for search
        let initial_search_pos = normalize_search_pos(&buffer.text(), session_snapshot.search_pos);
        let search_pos = Rc::new(RefCell::new(initial_search_pos));
        let last_search_text = Rc::new(RefCell::new(session_snapshot.last_search_text));

        let (sender, receiver) = std::sync::mpsc::channel::<DialogMessage>();

        // Find Next callback
        let sender_for_find = sender.clone();
        let find_input_clone = find_input.clone();
        let case_check_clone = case_check.clone();
        find_next_btn.set_callback(move |_| {
            let search_text = find_input_clone.value();
            if search_text.is_empty() {
                return;
            }

            let _ = sender_for_find.send(DialogMessage::FindNext {
                search_text,
                case_sensitive: case_check_clone.value(),
            });
            app::awake();
        });

        // Enter key in find input triggers Find Next
        let sender_for_find_enter = sender.clone();
        let find_input_enter = find_input.clone();
        let case_check_enter = case_check.clone();
        find_input.set_callback(move |_| {
            let search_text = find_input_enter.value();
            if search_text.is_empty() {
                return;
            }
            let _ = sender_for_find_enter.send(DialogMessage::FindNext {
                search_text,
                case_sensitive: case_check_enter.value(),
            });
            app::awake();
        });

        // Replace callback
        if let Some(mut replace_btn) = replace_btn {
            if let Some(replace_input_clone) = replace_input.clone() {
                let sender_for_replace = sender.clone();
                let find_input_clone = find_input.clone();
                let case_check_clone = case_check.clone();

                replace_btn.set_callback(move |_| {
                    let search_text = find_input_clone.value();
                    let replace_text = replace_input_clone.value();

                    if search_text.is_empty() {
                        return;
                    }

                    let _ = sender_for_replace.send(DialogMessage::Replace {
                        search_text,
                        replace_text,
                        case_sensitive: case_check_clone.value(),
                    });
                    app::awake();
                });
            } else {
                eprintln!("Replace input not available for replace action.");
            }
        }

        // Replace All callback
        if let Some(mut replace_all_btn) = replace_all_btn {
            if let Some(replace_input_clone) = replace_input.clone() {
                let sender_for_replace_all = sender.clone();
                let find_input_clone = find_input.clone();
                let case_check_clone = case_check.clone();

                replace_all_btn.set_callback(move |_| {
                    let search_text = find_input_clone.value();
                    let replace_text = replace_input_clone.value();

                    if search_text.is_empty() {
                        return;
                    }

                    let _ = sender_for_replace_all.send(DialogMessage::ReplaceAll {
                        search_text,
                        replace_text,
                        case_sensitive: case_check_clone.value(),
                    });
                    app::awake();
                });
            } else {
                eprintln!("Replace input not available for replace-all action.");
            }
        }

        // Close callback
        let sender_for_close = sender.clone();
        close_btn.set_callback(move |_| {
            let _ = sender_for_close.send(DialogMessage::Close);
            app::awake();
        });

        dialog.show();

        let mut buffer = buffer.clone();
        let mut editor = editor.clone();
        let find_input_state = find_input.clone();
        let replace_input_state = replace_input.clone();
        let case_check_state = case_check.clone();
        let search_pos_state = search_pos.clone();
        let last_search_text_state = last_search_text.clone();

        while dialog.shown() {
            fltk::app::wait();
            while let Ok(message) = receiver.try_recv() {
                match message {
                    DialogMessage::FindNext {
                        search_text,
                        case_sensitive,
                    } => {
                        if *last_search_text.borrow() != search_text {
                            *search_pos.borrow_mut() = 0;
                            *last_search_text.borrow_mut() = search_text.clone();
                        }
                        let text = buffer.text();
                        let start_pos = normalize_search_pos(&text, *search_pos.borrow());
                        *search_pos.borrow_mut() = start_pos;

                        if let Some((match_start, match_end)) =
                            find_next_match(&text, &search_text, start_pos, case_sensitive)
                        {
                            buffer.select(match_start as i32, match_end as i32);
                            editor.set_insert_position(match_end as i32);
                            editor.show_insert_position();
                            // Use match_end instead of match_start + 1 to avoid UTF-8 boundary issues
                            *search_pos.borrow_mut() = match_end.min(text.len()) as i32;
                        } else if start_pos > 0 {
                            *search_pos.borrow_mut() = 0;
                            fltk::dialog::message_default(
                                "Reached end, searching from beginning...",
                            );
                        } else {
                            fltk::dialog::message_default("Text not found");
                        }
                    }
                    DialogMessage::Replace {
                        search_text,
                        replace_text,
                        case_sensitive,
                    } => {
                        if *last_search_text.borrow() != search_text {
                            *last_search_text.borrow_mut() = search_text.clone();
                        }
                        if let Some((start, end)) = buffer.selection_position() {
                            let selected = buffer.text_range(start, end).unwrap_or_default();
                            let matches = if case_sensitive {
                                selected == search_text
                            } else {
                                selected.to_ascii_lowercase() == search_text.to_ascii_lowercase()
                            };

                            if matches {
                                buffer.remove(start, end);
                                buffer.insert(start, &replace_text);
                                let next_pos = start + replace_text.len() as i32;
                                editor.set_insert_position(next_pos);
                                *search_pos.borrow_mut() =
                                    normalize_search_pos(&buffer.text(), next_pos);
                            }
                        }
                    }
                    DialogMessage::ReplaceAll {
                        search_text,
                        replace_text,
                        case_sensitive,
                    } => {
                        if *last_search_text.borrow() != search_text {
                            *last_search_text.borrow_mut() = search_text.clone();
                        }
                        if search_text.is_empty() {
                            fltk::dialog::message_default("Search text is empty");
                            continue;
                        }
                        let text = buffer.text();
                        let new_text = if case_sensitive {
                            text.replace(&search_text, &replace_text)
                        } else {
                            let mut result = String::with_capacity(text.len());
                            let mut search_pos = 0usize;
                            while let Some((match_start, match_end)) =
                                find_next_match(&text, &search_text, search_pos as i32, false)
                            {
                                result.push_str(text.get(search_pos..match_start).unwrap_or(""));
                                result.push_str(&replace_text);
                                search_pos = match_end;
                                if search_pos >= text.len() {
                                    break;
                                }
                            }
                            result.push_str(text.get(search_pos..).unwrap_or(""));
                            result
                        };

                        let count = if case_sensitive {
                            text.matches(&search_text).count()
                        } else {
                            let mut count = 0usize;
                            let mut search_pos = 0usize;
                            while let Some((_match_start, match_end)) =
                                find_next_match(&text, &search_text, search_pos as i32, false)
                            {
                                count += 1;
                                search_pos = match_end;
                                if search_pos >= text.len() {
                                    break;
                                }
                            }
                            count
                        };

                        buffer.set_text(&new_text);
                        *search_pos.borrow_mut() = 0;
                        fltk::dialog::message_default(&format!("Replaced {} occurrences", count));
                    }
                    DialogMessage::Close => {
                        save_find_replace_state(
                            &find_input_state,
                            replace_input_state.as_ref(),
                            &case_check_state,
                            *search_pos_state.borrow(),
                            &last_search_text_state.borrow(),
                        );
                        dialog.hide();
                    }
                }

                if dialog.shown() {
                    save_find_replace_state(
                        &find_input_state,
                        replace_input_state.as_ref(),
                        &case_check_state,
                        *search_pos_state.borrow(),
                        &last_search_text_state.borrow(),
                    );
                }
            }
        }

        save_find_replace_state(
            &find_input_state,
            replace_input_state.as_ref(),
            &case_check_state,
            *search_pos_state.borrow(),
            &last_search_text_state.borrow(),
        );

        // Remove dialog from popups to prevent memory leak
        popups
            .borrow_mut()
            .retain(|w| w.as_widget_ptr() != dialog.as_widget_ptr());

        // Explicitly destroy top-level dialog widgets to release native resources.
        Window::delete(dialog);
    }

    pub fn find_next_from_session(editor: &mut TextEditor, buffer: &mut TextBuffer) -> bool {
        let session = FIND_REPLACE_SESSION.with(|state| state.borrow().clone());
        if session.find_text.is_empty() {
            return false;
        }

        let text = buffer.text();
        let start_pos = if session.last_search_text != session.find_text {
            0
        } else {
            normalize_search_pos(&text, session.search_pos)
        };

        let found = find_next_match(&text, &session.find_text, start_pos, session.case_sensitive)
            .or_else(|| {
                if start_pos > 0 {
                    find_next_match(&text, &session.find_text, 0, session.case_sensitive)
                } else {
                    None
                }
            });

        if let Some((match_start, match_end)) = found {
            buffer.select(match_start as i32, match_end as i32);
            editor.set_insert_position(match_end as i32);
            editor.show_insert_position();
            FIND_REPLACE_SESSION.with(|state| {
                let mut state = state.borrow_mut();
                state.last_search_text = session.find_text.clone();
                state.search_pos = match_end as i32;
            });
            true
        } else {
            false
        }
    }

    /// Find next occurrence (for F3 shortcut)
    #[allow(dead_code)]
    pub fn find_next(
        editor: &mut TextEditor,
        buffer: &mut TextBuffer,
        search_text: &str,
        case_sensitive: bool,
    ) -> bool {
        if search_text.is_empty() {
            return false;
        }

        let current_pos = editor.insert_position();
        let text = buffer.text();
        let start_pos = normalize_search_pos(&text, current_pos);

        if let Some((match_start, match_end)) =
            find_next_match(&text, search_text, start_pos, case_sensitive)
        {
            buffer.select(match_start as i32, match_end as i32);
            editor.set_insert_position(match_end as i32);
            editor.show_insert_position();
            true
        } else {
            // Try from beginning
            if let Some((match_start, match_end)) =
                find_next_match(&text, search_text, 0, case_sensitive)
            {
                buffer.select(match_start as i32, match_end as i32);
                editor.set_insert_position(match_end as i32);
                editor.show_insert_position();
                true
            } else {
                false
            }
        }
    }
}

fn find_next_match(
    text: &str,
    search_text: &str,
    start_pos: i32,
    case_sensitive: bool,
) -> Option<(usize, usize)> {
    if search_text.is_empty() || text.is_empty() {
        return None;
    }
    let start_pos = normalize_search_pos(text, start_pos) as usize;
    let Some(haystack) = text.get(start_pos..) else {
        return None;
    };
    if case_sensitive {
        let pos = haystack.find(search_text)?;
        let match_start = start_pos + pos;
        let match_end = match_start + search_text.len();
        return Some((match_start, match_end));
    }

    let pos = find_ascii_case_insensitive(haystack, search_text)?;
    let match_start = start_pos + pos;
    let match_end = match_start + search_text.len();
    Some((match_start, match_end))
}

fn find_ascii_case_insensitive(haystack: &str, needle: &str) -> Option<usize> {
    let haystack_bytes = haystack.as_bytes();
    let needle_bytes = needle.as_bytes();
    if needle_bytes.is_empty() || needle_bytes.len() > haystack_bytes.len() {
        return None;
    }

    let last = haystack_bytes.len() - needle_bytes.len();
    for idx in 0..=last {
        if !haystack.is_char_boundary(idx) {
            continue;
        }
        let end = idx + needle_bytes.len();
        if !haystack.is_char_boundary(end) {
            continue;
        }
        if haystack_bytes[idx..end]
            .iter()
            .zip(needle_bytes.iter())
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
        {
            return Some(idx);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::{find_next_match, normalize_search_pos};

    #[test]
    fn normalize_search_pos_clamps_non_boundary_utf8_offset() {
        let text = "ab한글cd";
        let mid_char_offset = text.find('한').expect("expected utf-8 anchor") + 1;
        let normalized = normalize_search_pos(text, mid_char_offset as i32);
        assert_eq!(normalized as usize, text.find('한').unwrap_or(0));
    }

    #[test]
    fn find_next_match_clamps_non_boundary_utf8_offset() {
        let text = "a한b한c";
        let second_han = text.rfind('한').expect("expected second utf8 anchor");
        let mid_second_han = second_han + 1;
        let (start, end) = find_next_match(text, "한", mid_second_han as i32, true)
            .expect("expected to find second match");
        assert_eq!(start, second_han);
        assert_eq!(end, second_han + "한".len());
    }

    #[test]
    fn find_next_match_case_insensitive_handles_utf8_with_byte_scan() {
        let text = "가A나a";
        let first_ascii = text.find('A').expect("expected first ascii letter");
        let second_ascii = text.rfind('a').expect("expected second ascii letter");

        let first = find_next_match(text, "a", 0, false).expect("expected first match");
        assert_eq!(first, (first_ascii, first_ascii + "A".len()));

        let second =
            find_next_match(text, "A", first.1 as i32, false).expect("expected second match");
        assert_eq!(second, (second_ascii, second_ascii + "a".len()));
    }
}
