from codepack.utils.looper import Looper
import pytest
import time


def immediate_exit():
    raise KeyboardInterrupt()


def error_function():
    raise ValueError()


def test_redundant_start():
    try:
        looper = Looper(function=immediate_exit, interval=0.5)
        looper.start()
        with pytest.raises(Exception):
            looper.start()
    finally:
        looper.stop()


def test_if_looper_is_running_in_background():
    try:
        looper = Looper(function=immediate_exit, interval=0.5)
        assert not looper.is_running()
        looper.start()
        assert looper.is_running()
        time.sleep(1.0)
        assert not looper.is_running()
    finally:
        looper.stop()


def test_restart1():
    try:
        looper = Looper(function=immediate_exit, interval=0.1)
        looper.start()
        assert looper.is_running()
        time.sleep(0.5)
        assert not looper.is_running()
        looper.restart()
        assert looper.is_running()
    finally:
        looper.stop()


def test_restart2():
    try:
        looper = Looper(function=immediate_exit, interval=0.1)
        looper.start()
        looper.restart()
        assert looper.is_running()
    finally:
        looper.stop()


def test_if_thread_is_alive_after_exception_raises():
    try:
        looper = Looper(function=error_function, interval=0.1)
        looper.start()
        assert looper.is_running()
        assert looper.is_alive()
        time.sleep(0.5)
        assert not looper.is_running()
        assert not looper.is_alive()
    finally:
        looper.stop()


def test_if_thread_is_alive_after_keyboard_interrupt_occurs():
    try:
        looper = Looper(function=immediate_exit, interval=0.1)
        looper.start()
        assert looper.is_running()
        assert looper.is_alive()
        time.sleep(0.5)
        assert not looper.is_running()
        assert not looper.is_alive()
    finally:
        looper.stop()


def test_if_foreground_looper_is_alive():
    try:
        looper = Looper(function=immediate_exit, interval=0.1, background=False)
        assert not looper.is_alive()
        assert not looper.is_running()
        looper.start()
        assert not looper.is_alive()
        assert not looper.is_running()
    finally:
        looper.stop()
