import chaosagent_control_plane


def test_control_plane_package_is_importable() -> None:
    """Prove that the Python test and workspace installation pipelines work."""
    assert chaosagent_control_plane.__doc__
