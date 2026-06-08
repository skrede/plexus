Do not add Co-Authored-By lines to commit messages.

Do not be sycophantic nor agreeable to be appealing to the user.
The user values rigor, honesty and objectivity -- not a sycophancy.

Use american English: e.g., "stabilizing" not "stabilising", and "color" not "colour"

Never refer to issues, IDs and keys that are result of planning tools, project management tools, or issue trackers
(including phase numbers, milestones, plans or task numbers or any other kind of planning artifact ID/key -- including from GSD and .planning artifacts)
in any area of the code/bodebase, including but not limited to commit messages, code, code comments, documentation, and examples.

Never do git tagging, never merge and no force flags to circumvent .gitignore.

Commit message format:
{Prefix}: {summary sentence}.

- {what was done, one line per item}
- {another item if applicable}

Allowed prefixes: Feature, Fix, Refactor, Docs, Examples, Optimization, WIP
The summary line should be brief and descriptive. The bullet list expands on what was done.
Single-item commits may omit the bullet list.

There should be one commit per GSD plan within each GSD phase (so a phase with 3 plans has at least 3 commits);
you can make additional commits if you need them for safekeeping during development, but you should by default attempt
to make one commit per plan (not GSD task or other things). Use the WIP prefix if the code in the commit does not
compile.

No issue tags, phase numbers, or planning tool references in commit messages, code, comments, docs, or examples.

Branching model: master (releases) ? develop (integration) ? milestone/<version> (work).
Create milestone branches from the current branch, named milestone/<version> (e.g., milestone/v0.2.0).
If develop does not exist, create it from master.
Merge path: milestone ? develop ? master. Never delete develop.
You can make commits to the milestone branch but never push.

.planning/ files should not be committed to the code project repository.
Do not override .gitignore to attempt to add files or folders, including .planning/.
./planning should have a separate shadow repository initialized (without submodules and independent of the code project)
where the state is kept.
If there is no such repo, leave the files to be locally.

Generate idiomatic, cross-platform C++20 code -- the code must run on macOS, Linux and Windows. Leverage the language features as much as possible!

Adhere to typical conventions of popular and modern C++ libraries, e.g., asio, boost, and so on;
existing conventions in the project take precedence, if you are unsure about conventions, or existing conventions
contradict typical and popular conventions, ask the user what to do.

Use header guards and not pragma once. Header guards are on the format HPP_GUARD_<NAMESPACE>_<FOLDER>_FILENAME_H.
Do not add matching comment // namespace "the namespace" after closing namespace brackets, e.g., } //
namespace {namespace name}
Do not add matching comment // HPP_GUARD_... define macro after the include guard #endif

The #include order for the project is as follows (unless order matter for other reasons like something must come before
something else):

- internal project includes (#include with ") come at the top, third-party libs come second, and standard library
  headers come third.
- these three major "sections" are divided by a new blank line. Within the major sections, includes are grouped by
  folder location to form intermediate sections, which are separated by a blank line.
- Only one blank line between sections, even if a new major starts
- For each "section" of includes, all includes are sorted first by length, then with the same length alphabetically.