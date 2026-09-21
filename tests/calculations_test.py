import sys
import os

# Installed Modules
import pytest

# Project Modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from calculations import area_of_circle, get_nth_fibonacci  # noqa: E402


def test_area_of_circle_positive_radius():
    """Test with a positive radius."""
    result = area_of_circle(1)
    assert abs(result - 3.14159) < 1e-5


def test_area_of_circle_zero_radius():
    """Test with a radius of zero."""
    assert area_of_circle(0) == 0


def test_area_of_circle_negative_radius():
    """Test that negative radii are rejected."""
    with pytest.raises(ValueError, match="Radius cannot be negative"):
        area_of_circle(-1)


def test_get_nth_fibonacci_zero():
    """Test with n=0."""
    assert get_nth_fibonacci(0) == 0


def test_get_nth_fibonacci_one():
    """Test with n=1."""
    assert get_nth_fibonacci(1) == 1


def test_get_nth_fibonacci_ten():
    """Test with n=10."""
    assert get_nth_fibonacci(10) == 55


def test_get_nth_fibonacci_negative():
    """Test that negative indexes are rejected."""
    with pytest.raises(ValueError, match="n cannot be negative"):
        get_nth_fibonacci(-1)
