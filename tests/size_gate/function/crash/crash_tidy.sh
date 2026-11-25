#!/bin/sh
# Runs CRASH_TIDY_REAL, except on a source named crash_*, where it dies the way a crashing clang-tidy does: every time
# under CRASH_TIDY_MODE=always, else once, recorded by creating CRASH_TIDY_STAMP. SIGKILL leaves no core dump.
for source in "$@"; do :; done
case "${source##*/}" in
crash_*)
    if [ "$CRASH_TIDY_MODE" = always ] || [ ! -e "$CRASH_TIDY_STAMP" ]; then
        : > "$CRASH_TIDY_STAMP"
        printf '%s\n' "PLEASE submit a bug report to https://github.com/llvm/llvm-project/issues/ and include the crash backtrace." >&2
        printf 'Stack dump:\n0.\tProgram arguments: %s\n' "$0 $*" >&2
        printf '%s\n' " #0 0x0000000000000000 llvm::sys::PrintStackTrace(llvm::raw_ostream&, int)" >&2
        kill -KILL $$
    fi
    ;;
esac
exec "$CRASH_TIDY_REAL" "$@"
