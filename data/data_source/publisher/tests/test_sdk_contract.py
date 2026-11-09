"""Verify installed optional SDK APIs with local stubs and no service connections."""

import io

import pytest

from data.data_source.publisher.dataset import SourceFile
from data.data_source.publisher.transports import KafkaTransport, S3Transport


@pytest.mark.parametrize("multipart", [False, True])
def test_real_boto3_upload_api_with_stubbed_service(settings, tmp_path, monkeypatch, multipart):
    """Exercise boto3 transfer APIs without loading ambient credentials or making requests."""
    boto3 = pytest.importorskip("boto3")
    stub = pytest.importorskip("botocore.stub")
    for name in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_SESSION_TOKEN",
                 "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "publisher-test-dummy-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "publisher-test-dummy-secret")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "absent-credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "absent-config"))
    monkeypatch.setattr(boto3, "DEFAULT_SESSION", None)
    transport = S3Transport(settings, "a" * 64)
    content = b"x" * (9 * 1024 * 1024) if multipart else b"unchanged-source-bytes\n"
    item = SourceFile("offline/initial/Trade.txt", 1, len(content), "b" * 64)
    calls = []

    def capture(params, **keywords):
        """Capture SDK-generated parameters before the stubbed request is handled."""
        calls.append(params)

    operation = "CreateMultipartUpload" if multipart else "PutObject"
    transport.client.meta.events.register("before-parameter-build.s3." + operation, capture)
    with stub.Stubber(transport.client) as service:
        if multipart:
            service.add_response("create_multipart_upload", {"UploadId": "stub-upload"})
            service.add_response("upload_part", {"ETag": '"part-one"'})
            service.add_response("upload_part", {"ETag": '"part-two"'})
            service.add_response("complete_multipart_upload", {"ETag": '"multipart-not-a-source-hash"'})
        else:
            service.add_response("put_object", {"ETag": '"not-a-source-hash"'})
        transport.upload(item, io.BytesIO(content))
        service.assert_no_pending_responses()
    transport.close()
    assert calls[0]["Bucket"] == "upstream" and calls[0]["Key"] == "initial/Trade.txt"
    assert calls[0]["Metadata"]["publisher-source-sha256"] == item.sha256


def test_real_confluent_constructor_accepts_adapter_settings_without_brokers(settings, monkeypatch):
    """Validate native producer settings while deliberately removing all bootstrap servers."""
    module = pytest.importorskip("confluent_kafka")
    native_producer = module.Producer
    captured = {}

    def local_producer(config, **keywords):
        """Construct the real client with no hosts so it cannot connect to a service."""
        config = dict(config)
        config["bootstrap.servers"] = ""
        captured.update(config)
        return native_producer(config, **keywords)

    monkeypatch.setattr(module, "Producer", local_producer)
    transport = KafkaTransport(settings)
    transport.begin_batch()
    transport.finish_batch()
    transport.close()
    assert captured["bootstrap.servers"] == ""
    assert captured["enable.idempotence"] is True
    assert captured["queue.buffering.max.messages"] == 256
