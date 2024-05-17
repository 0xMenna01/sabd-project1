import os


def create_dir(path: str) -> None:
    """Create a directory if it does not exist."""
    if not os.path.exists(path):
        os.makedirs(path)


def load_bytes_from(path: str) -> bytes:
    """Load the content of a file."""
    with open(path, 'rb') as file:
        # Read the entire file
        content = file.read()

    return content


def write_file(path: str, content: str) -> None:
    """Write content to a file."""
    with open(path, "w") as file:
        file.write(content)


def delete_file(path: str) -> None:
    """Delete a file if it exists."""
    if os.path.exists(path):
        os.remove(path)
