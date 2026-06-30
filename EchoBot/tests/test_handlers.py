from __future__ import annotations

import asyncio
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from aiogram.types import Chat, Message, MessageId, User

from database import Database
from handlers import build_router


class RelayHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_directory = tempfile.TemporaryDirectory()
        self.database = Database(Path(self.temp_directory.name) / "test.db")
        await self.database.connect()
        for user_id in (1, 2):
            await self.database.upsert_user(user_id, f"user{user_id}", f"User {user_id}")
            await self.database.confirm_adult(user_id)
        await self.database.enqueue_or_match(1)
        self.match = await self.database.enqueue_or_match(2)

    async def asyncTearDown(self) -> None:
        await self.database.close()
        for attempt in range(10):
            try:
                self.temp_directory.cleanup()
                break
            except OSError:
                if attempt == 9:
                    raise
                await asyncio.sleep(0.05)

    async def test_reply_is_delivered_as_reply_to_matching_partner_message(self) -> None:
        await self.database.record_relayed_message(
            self.match.dialog_id,
            sender_id=1,
            sender_message_id=101,
            recipient_id=2,
            recipient_message_id=201,
        )
        now = datetime.now(timezone.utc)
        chat = Chat(id=2, type="private")
        user = User(id=2, is_bot=False, first_name="User 2")
        message = Message(
            message_id=202,
            date=now,
            chat=chat,
            from_user=user,
            text="Ответ",
            reply_to_message=Message(
                message_id=201,
                date=now,
                chat=chat,
                from_user=User(id=999, is_bot=True, first_name="ЭХО"),
                text="Исходное сообщение",
            ),
        )
        bot = Mock()
        bot.copy_message = AsyncMock(return_value=MessageId(message_id=102))
        router = build_router(self.database, Mock(), Mock())
        relay_handler = next(
            handler.callback
            for handler in router.message.handlers
            if handler.callback.__name__ == "relay_handler"
        )

        await relay_handler(message, bot)

        call = bot.copy_message.await_args
        self.assertEqual(call.kwargs["chat_id"], 1)
        self.assertEqual(call.kwargs["reply_parameters"].message_id, 101)
        self.assertTrue(call.kwargs["reply_parameters"].allow_sending_without_reply)
        self.assertEqual(
            await self.database.get_relayed_reply_target(
                self.match.dialog_id, user_id=2, message_id=202, partner_id=1
            ),
            102,
        )

    async def test_new_user_notification_is_sent_only_by_admin_bot(self) -> None:
        user_id = 3
        admin_id = 999
        await self.database.upsert_user(user_id, "new_user", "New User")

        message = Mock()
        message.from_user = User(
            id=user_id,
            is_bot=False,
            first_name="New User",
            username="new_user",
        )
        message.answer = AsyncMock()

        echo_bot = Mock()
        echo_bot.send_message = AsyncMock()
        admin_bot = Mock()
        admin_bot.send_message = AsyncMock()
        settings = SimpleNamespace(
            admin_ids=frozenset({admin_id}),
            admin_usernames=frozenset(),
        )
        router = build_router(
            self.database,
            settings,
            Mock(),
            admin_bot=admin_bot,
        )
        start_handler = next(
            handler.callback
            for handler in router.message.handlers
            if handler.callback.__name__ == "start_handler"
        )

        await start_handler(
            message,
            SimpleNamespace(args="campaign"),
            echo_bot,
        )

        admin_bot.send_message.assert_awaited_once()
        self.assertEqual(admin_bot.send_message.await_args.args[0], admin_id)
        self.assertIn("campaign", admin_bot.send_message.await_args.args[1])
        echo_bot.send_message.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
