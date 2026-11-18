def test_imports() -> None:
    import tickstore  # noqa: F401


def test_alpaca_feed_imports() -> None:
    from tickstore.ingest import alpaca_feed  # noqa: F401
