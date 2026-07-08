"""
Tests for the DCM1 protocol command builders.
"""

import pytest
from pydcm1.protocol import MixerProtocol


# ---------------------------------------------------------------------------
# Paging command builder tests
# ---------------------------------------------------------------------------

def test_command_paging_open_zone1():
    """Zone 1 paging open should produce the correct wire format."""
    assert MixerProtocol.command_paging_open("XOOOOOOO") == "<PM,PAXOOOOOOO/>\r"


def test_command_paging_open_zone8():
    """Zone 8 paging open should produce the correct wire format."""
    assert MixerProtocol.command_paging_open("OOOOOOOX") == "<PM,PAOOOOOOOX/>\r"


def test_command_paging_close_all():
    """Paging release (close all) should produce the correct wire format."""
    assert MixerProtocol.command_paging_close_all() == "<PM,PR/>\r"


# ---------------------------------------------------------------------------
# Existing placeholder (kept for CI stability)
# ---------------------------------------------------------------------------

def test_placeholder():
    """Placeholder — ensures the test suite always has at least one passing test."""
    assert True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
