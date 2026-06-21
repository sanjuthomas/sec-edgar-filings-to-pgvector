import pytest

from edgar_etl.config import Settings
from edgar_etl.kafka_manager import ConsumerState, KafkaConsumerManager


def test_kafka_manager_initial_state() -> None:
    manager = KafkaConsumerManager(Settings())
    status = manager.status()
    assert status["state"] == "stopped"
    assert status["offset_mode"] is None
    assert not manager.is_running


def test_kafka_manager_start_when_running_raises() -> None:
    manager = KafkaConsumerManager(Settings())
    manager._state = ConsumerState.RUNNING
    with pytest.raises(RuntimeError, match="already running"):
        manager.start("earliest")
