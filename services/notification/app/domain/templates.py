"""Filling a template's {placeholders} (pure). Uses format_map with a forgiving dict: a missing
variable shows as "{name}" instead of crashing, so a template edit can't break sending."""


class _Forgiving(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def render(subject: str, body: str, variables: dict) -> tuple[str, str]:
    values = _Forgiving(variables)
    # Subjects are one line: a newline in a value must not inject extra email headers
    return subject.format_map(values).replace("\r", " ").replace("\n", " "), body.format_map(values)
