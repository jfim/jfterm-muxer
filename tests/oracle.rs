use jftermd_core::scanner_test_support::Scanner;

/// Render a byte stream into a fixed-size vt100 screen and return a stable
/// representation (formatted contents + cursor) for equality comparison.
fn render(bytes: &[u8]) -> (Vec<u8>, (u16, u16)) {
    let mut p = vt100::Parser::new(24, 80, 0);
    p.process(bytes);
    (
        p.screen().contents_formatted(),
        p.screen().cursor_position(),
    )
}

/// Assert that replaying `input` reproduces the same screen as `input` itself.
fn assert_replay_faithful(input: &[u8]) {
    let mut s = Scanner::new(128 * 1024);
    s.feed(input);
    let replay = s.replay(usize::MAX);

    let original = render(input);
    let replayed = render(&replay);
    assert_eq!(
        original,
        replayed,
        "replay diverged from original\ninput={:?}\nreplay={:?}",
        String::from_utf8_lossy(input),
        String::from_utf8_lossy(&replay),
    );
}

#[test]
fn plain_text() {
    assert_replay_faithful(b"the quick brown fox\r\njumps over\r\n");
}

#[test]
fn colored_and_styled_text() {
    assert_replay_faithful(b"\x1b[1;31mred bold\x1b[0m normal \x1b[4munder\x1b[0m");
}

#[test]
fn cursor_movement_and_overwrite() {
    assert_replay_faithful(b"hello\x1b[1;1Hxxxxx\x1b[2;1Hworld");
}

#[test]
fn scroll_region_and_newlines() {
    assert_replay_faithful(b"\x1b[2;23r\x1b[5;1Hline\r\nanother\r\nthird\r\n");
}

#[test]
fn clipboard_write_does_not_affect_screen() {
    assert_replay_faithful(b"before\x1b]52;c;c2VjcmV0\x07after");
}

#[test]
fn bell_and_queries_do_not_affect_screen() {
    assert_replay_faithful(b"a\x07b\x1b[6nc\x1b[cd");
}

#[test]
fn title_and_cwd_then_text() {
    assert_replay_faithful(b"\x1b]2;mytitle\x07\x1b]7;file:///tmp\x07content here");
}

#[test]
fn clear_then_redraw() {
    assert_replay_faithful(b"garbage to be cleared\x1b[2J\x1b[Hfresh screen content");
}

#[test]
fn capped_replay_keeps_bottom_screen_faithful() {
    let mut s = Scanner::new(16);
    let input = b"\x1b[32mAAAAAAAA\r\nBBBBBBBB\r\nCCCCCCCC\r\n\x1b[0mDDDD";
    s.feed(input);
    let capped = s.replay(1);

    let mut p = vt100::Parser::new(24, 80, 0);
    p.process(&capped);
    let contents = p.screen().contents();
    assert!(
        contents.contains("DDDD"),
        "capped replay lost the final line: {contents:?}"
    );
}
