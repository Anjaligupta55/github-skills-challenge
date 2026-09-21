
from pathlib import Path
import json
import runpy

from src.anomaly_detector import AnomalyDetector
from src.aiops_pipeline import run_pipeline
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.event_topic import EventTopic


def test_normal_record_is_not_anomaly():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 42,
        "memory_percent": 51,
        "log_level": "INFO",
        "message": "Payment request processed successfully"
    }

    assert detector.detect(record) is None


def test_anomalous_record_is_detected():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 610,
        "cpu_percent": 75,
        "memory_percent": 70,
        "log_level": "ERROR",
        "message": "Payment service timeout"
    }

    event = detector.detect(record)

    assert event is not None
    assert event["type"] == "ANOMALY"


def test_producer_publishes_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    assert producer.publish(event)
    assert len(topic.get_messages()) == 1


def test_consumer_receives_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)
    consumer = EventConsumer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    producer.publish(event)

    messages = consumer.consume()

    assert len(messages) == 1

def test_pipeline_processes_normal_and_anomalous_records(tmp_path):
    data_file = tmp_path / "service_data.json"
    records = [
        {
            "timestamp": "2026-09-20T10:00:00",
            "service": "payment-service",
            "response_time_ms": 100,
            "cpu_percent": 20,
            "memory_percent": 30,
            "log_level": "INFO",
            "message": "Request completed"
        },
        {
            "timestamp": "2026-09-20T10:05:00",
            "service": "payment-service",
            "response_time_ms": 700,
            "cpu_percent": 90,
            "memory_percent": 95,
            "log_level": "WARNING",
            "message": "Service is unhealthy"
        }
    ]
    data_file.write_text(json.dumps(records), encoding="utf-8")

    result = run_pipeline(str(data_file))

    assert result["records_processed"] == 2
    assert len(result["anomalies_detected"]) == 1
    assert result["anomalies_detected"][0]["type"] == "ANOMALY"
    assert len(result["events_consumed"]) == 1
    assert result["events_consumed"][0]["type"] == "ANOMALY"


def test_pipeline_script_runs(tmp_path, monkeypatch, capsys):
    data_directory = tmp_path / "data"
    data_directory.mkdir()

    records = [
        {
            "timestamp": "2026-09-20T10:00:00",
            "service": "payment-service",
            "response_time_ms": 100,
            "cpu_percent": 20,
            "memory_percent": 30,
            "log_level": "INFO",
            "message": "Request completed"
        }
    ]

    (data_directory / "service_data.json").write_text(
        json.dumps(records),
        encoding="utf-8"

        
    )

    monkeypatch.chdir(tmp_path)

    runpy.run_path(
        str(Path(__file__).parents[1] / "src" / "aiops_pipeline.py"),
        run_name="__main__"
    )

    output = capsys.readouterr().out
    assert "AIOps Pipeline Result" in output
    assert "Records processed: 1" in output
    assert "Anomalies detected: 0" in output


def test_detector_detects_all_anomaly_reasons():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:10:00",
        "service": "payment-service",
        "response_time_ms": 501,
        "cpu_percent": 81,
        "memory_percent": 81,
        "log_level": "WARNING",
        "message": "Multiple service thresholds exceeded"
    }

    event = detector.detect(record)

    assert event["reasons"] == [
        "High response time",
        "High CPU utilization",
        "High memory utilization",
        "Error log detected"
    ]


def test_producer_rejects_empty_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    assert producer.publish(None) is False
    assert topic.get_messages() == []


def test_topic_clear_removes_messages():
    topic = EventTopic("anomaly-events")
    event = {"type": "ANOMALY"}

    topic.publish(event)
    assert topic.get_messages() == [event]

    topic.clear()

    assert topic.get_messages() == []