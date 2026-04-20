from unittest.mock import MagicMock, patch

import lambdas.analytics.handler as handler


@patch("lambdas.analytics.handler.boto3.client")
@patch("lambdas.analytics.handler.boto3.resource")
def test_notify_watchlist_queries_hub_index_and_emails_notification_address(mock_resource, mock_client):
    mock_ses = MagicMock()
    mock_client.return_value = mock_ses

    mock_table = MagicMock()
    mock_table.query.return_value = {
        "Items": [
            {
                "user_id": "user-123",
                "hub_id": "H001",
                "notification_email": "user@example.com",
            }
        ]
    }
    mock_messages = MagicMock()
    mock_resource.return_value.Table.side_effect = [mock_table, mock_messages]

    handler.notify_watchlist("H001")

    mock_table.query.assert_called_once()
    query_kwargs = mock_table.query.call_args.kwargs
    assert query_kwargs["IndexName"] == "hub-id-index"
    mock_ses.send_email.assert_called_once()
    assert mock_ses.send_email.call_args.kwargs["Destination"] == {
        "ToAddresses": ["user@example.com"]
    }
    mock_messages.put_item.assert_called_once()
