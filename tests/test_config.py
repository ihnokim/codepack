from codepack.utils.config import Config
from tests import test_config_path
import pytest
import os


def test_getting_config_path_when_nothing_is_given():
    config = Config()
    assert config.get_config_path() is None


def test_getting_config_path_when_os_env_is_given(os_env_test_config_path):
    config = Config()
    assert config.get_config_path() == os_env_test_config_path


def test_getting_confug_path_when_path_is_given():
    path = test_config_path
    config = Config(config_path=path)
    assert config.get_config_path() == path


def test_getting_config_path_when_invalid_path_is_given():
    path = 'invalid.ini'
    with pytest.raises(FileNotFoundError):
        _ = Config(config_path=path)


def test_getting_config_path_when_invalid_os_env_is_given():
    try:
        os.environ['CODEPACK_CONFIG_PATH'] = 'invalid.ini'
        with pytest.raises(FileNotFoundError):
            _ = Config()
    finally:
        os.environ.pop('CODEPACK_CONFIG_PATH', None)


def test_generating_os_env():
    assert Config.os_env('foo', 'bar') == 'CODEPACK__FOO__BAR'


def test_trying_generating_os_env_with_invalid_characters():
    assert Config.os_env('fo.o', 'b-ar') == 'CODEPACK__FO_O__B_AR'
    assert Config.os_env('fo@o', 'b/ar') == 'CODEPACK__FO_O__B_AR'


def test_trying_generating_os_env_with_invalid_characters_at_wrong_position():
    with pytest.raises(ValueError):
        Config.os_env('fo.o.', 'b-ar')
    with pytest.raises(ValueError):
        Config.os_env('.fo.o', 'b-ar')
    with pytest.raises(ValueError):
        Config.os_env('fo.o', '-b-ar')
    with pytest.raises(ValueError):
        Config.os_env('fo.o', 'b-ar-')


def test_trying_generating_os_env_with_consecutive_invalid_characaters():
    with pytest.raises(ValueError):
        Config.os_env('fo..o', 'bar')
    with pytest.raises(ValueError):
        Config.os_env('foo', 'b–-ar')
    with pytest.raises(ValueError):
        Config.os_env('fo.-o', 'bar')
    with pytest.raises(ValueError):
        Config.os_env('foo', 'b-_ar')


def test_parsing_config_sections(os_env_test_config_path):
    settings = Config.parse_config(path=os_env_test_config_path)
    assert set(settings.keys()) == {'test_section1', 'empty_section'}


def test_parsing_empty_config(os_env_test_config_path):
    settings = Config.parse_config(path=os_env_test_config_path)
    assert settings['empty_section'] == {}


def test_if_os_env_is_valid():
    assert Config.verify_os_env('CODEPACK__ABCD__EFGH') is True
    assert Config.verify_os_env('CODEPACK__ABCD1__EFGH') is True
    assert Config.verify_os_env('CODEPACK__ABCD__EFGH1') is True
    assert Config.verify_os_env('CODEPACK__AB_CD__EFGH') is True
    assert Config.verify_os_env('CODEPACK__ABCD__EFG_H') is True


def test_if_os_env_is_invalid_because_of_token_length():
    assert Config.verify_os_env('CODEPACK__') is False
    assert Config.verify_os_env('CODEPACK__ABCD_EFGH') is False


def test_if_os_env_is_invalid_because_of_using_invalid_characters():
    assert Config.verify_os_env('CODEPACK__AB.CD__EFGH') is False
    assert Config.verify_os_env('CODEPACK__ABCD__EFG-H') is False


def test_if_os_env_is_invalid_because_of_using_lower_case():
    assert Config.verify_os_env('CODEPACK__AbCD__EFGH') is False
    assert Config.verify_os_env('CODEPACK__ABCD__EFGh') is False


def test_if_os_env_is_invalid_because_of_using_underscore_at_both_ends():
    assert Config.verify_os_env('CODEPACK__ABCD___EFGH') is False
    assert Config.verify_os_env('CODEPACK___ABCD__EFGH') is False
    assert Config.verify_os_env('CODEPACK__ABCD__EFGH_') is False


def test_if_os_env_is_invalid_because_the_first_character_is_digit():
    assert Config.verify_os_env('CODEPACK__1ABCD__EFGH') is False
    assert Config.verify_os_env('CODEPACK__ABCD__2EFGH') is False


def test_initialization_without_anything():
    config = Config()
    assert config.snapshot == {}


def test_initialization_when_config_path_argument_is_given():
    config_path = test_config_path
    config = Config(config_path=config_path)
    assert config.snapshot == Config.parse_config(path=config_path)


def test_initialization_when_config_path_os_env_is_given(os_env_test_config_path):
    config = Config()
    assert config.snapshot == Config.parse_config(path=os_env_test_config_path)


def test_initialization_when_some_os_envs_are_detected(os_env_test_config_path):
    try:
        os_env1 = Config.os_env(section='new-section', key='test.key1')
        os_env2 = Config.os_env(section='test_section1', key='test@key2')
        os_env3 = Config.os_env(section='empty_section', key='test_key3')
        os_env4 = Config.os_env(section='test_section1', key='test_int')
        os.environ[os_env1] = 'test.value1'
        os.environ[os_env2] = 'test.value2'
        os.environ[os_env3] = 'test.value3'
        os.environ[os_env4] = '3'
        config = Config()
        _config = Config.parse_config(path=os_env_test_config_path)
        _config['new_section'] = {'test_key1': 'test.value1'}
        _config['test_section1']['test_key2'] = 'test.value2'
        _config['empty_section']['test_key3'] = 'test.value3'
        _config['test_section1']['test_int'] = '3'
        assert config.snapshot == _config
    finally:
        os.environ.pop(os_env1, None)
        os.environ.pop(os_env2, None)
        os.environ.pop(os_env3, None)
        os.environ.pop(os_env4, None)


def test_if_refresh_does_not_change_snapshot_when_config_apth_os_env_is_given():
    try:
        config_path = test_config_path
        os_env = 'CODEPACK_CONFIG_PATH'
        config = Config()
        assert config.snapshot == {}
        os.environ[os_env] = config_path
        config.refresh()
        assert config.snapshot == {}
    finally:
        os.environ.pop(os_env, None)


def test_if_refresh_does_not_change_snapshot_when_config_path_os_env_is_given_and_applied_to_config_instance():
    try:
        config_path = test_config_path
        os_env = 'CODEPACK_CONFIG_PATH'
        config = Config()
        assert config.snapshot == {}
        os.environ[os_env] = config_path
        config.set_config_path()
        config.refresh()
        assert config.snapshot == Config.parse_config(path=config_path)
    finally:
        os.environ.pop(os_env, None)


def test_if_refresh_changes_snapshot_when_some_os_env_is_detected():
    try:
        os_env = Config.os_env(section='new-section', key='test.key')
        config = Config()
        assert config.snapshot == {}
        os.environ[os_env] = 'test.value'
        config.refresh()
        assert config.snapshot == {'new_section': {'test_key': 'test.value'}}
    finally:
        os.environ.pop(os_env, None)


def test_getting_config(os_env_test_config_path):
    config = Config()
    _config = config.get_config(section='test_section1')
    assert _config == {'test_key': 'test-value', 'test_int': '1'}


def test_list_sections(os_env_test_config_path):
    config = Config()
    sections = set(config.list_sections())
    assert sections == {'test_section1', 'empty_section'}


def test_list_keys(os_env_test_config_path):
    config = Config()
    keys = set(config.list_keys(section='test_section1'))
    assert keys == {'test_key', 'test_int'}


def test_if_empty_key_os_env_is_valid():
    assert Config.verify_os_env(os_env='CODEPACK__SECTION__')


def test_if_empty_key_os_env_is_included_in_valid_os_envs():
    try:
        os_env1 = 'CODEPACK__SECTION'
        os_env2 = 'CODEPACK__SECTION__'
        os.environ[os_env1] = ''
        os.environ[os_env2] = ''
        assert Config.collect_valid_os_envs() == [os_env2]
    finally:
        os.environ.pop(os_env1, None)
        os.environ.pop(os_env2, None)


def test_if_empty_key_os_env_is_parsed_as_empty_dict():
    try:
        os_env1 = 'CODEPACK__SECTION1__'
        os_env2 = 'CODEPACK__SECTION2__KEY'
        os.environ[os_env1] = 'value1'
        os.environ[os_env2] = 'value2'
        config = Config()
        assert set(Config.collect_valid_os_envs()) == {os_env1, os_env2}
        assert config.snapshot == {'section1': {}, 'section2': {'key': 'value2'}}
    finally:
        os.environ.pop(os_env1, None)
        os.environ.pop(os_env2, None)


def test_if_empty_key_os_env_is_made_correctly():
    assert Config.os_env('SECTION1') == 'CODEPACK__SECTION1__'
