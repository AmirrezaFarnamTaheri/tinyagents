use super::ShellFlavor;

pub(super) fn shell_description(flavor: ShellFlavor) -> &'static str {
    match flavor {
        ShellFlavor::Cmd => {
            "Execute a command under Windows cmd.exe (`cmd /C`), not a POSIX shell: run code, \
             manipulate workspace files, or launch applications (`start \"\" music:`). Use cmd \
             syntax: `cd` with no argument prints the current directory (no `pwd`), `dir` lists \
             (no `ls`), `type` prints a file (no `cat`), `findstr` searches (no `grep`), \
             `del`/`copy`/`move` replace rm/cp/mv, variables are `%VAR%` (not `$VAR`), and \
             commands chain with `&&` (`;` is not a separator). For PowerShell run \
             `powershell -NoProfile -Command \"...\"`. Only stdout/stderr comes back, so a script \
             that computes silently or only writes a file returns nothing — print what you need, \
             or read the file afterwards."
        }
        ShellFlavor::Posix => {
            "Execute a shell command: run code, manipulate workspace files, or launch applications (`open -a Music`, `xdg-open music://`). Only stdout/stderr comes back, so a script that computes silently or only writes a file returns nothing — print what you need, or read the file afterwards."
        }
    }
}

pub(super) fn command_param_description(flavor: ShellFlavor) -> &'static str {
    match flavor {
        ShellFlavor::Cmd => "The cmd.exe command to execute (cmd syntax, not POSIX)",
        ShellFlavor::Posix => "The shell command to execute",
    }
}
