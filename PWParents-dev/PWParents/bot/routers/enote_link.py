# bot/routers/enote_link.py
from __future__ import annotations
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from core.db import get_conn
from core.repositories.clients import ClientsRepository
from core.integrations import enote
from core.logging_setup import get_enote_link_logger

router = Router(name="enote_link")
log = get_enote_link_logger()

class LinkStates(StatesGroup):
    waiting_contract = State()
    confirming = State()

def _topic_client_id_or_none(msg: Message) -> int | None:
    """
    Витягти client_user_id із контексту теми/реплаю.
    Найпростіший варіант: якщо команда виконується reply на повідомлення клієнта —
    беремо msg.reply_to_message.from_user.id як client_user_id.
    За потреби замініть на ваш сервіс мапінгу 'topic -> client_user_id'.
    """
    if msg.reply_to_message and msg.reply_to_message.from_user:
        return msg.reply_to_message.from_user.id
    return None

def _preview_text(owner_name: str, owner_phone: str, owner_ref: str, pets: list[dict]) -> str:
    lines = [
        f"👤 Власник (Єнот): {owner_name or '—'}",
        f"📞 Телефон (Єнот): {owner_phone or '—'}",
        f"🔗 owner_ref_key: {owner_ref}",
        "🐾 Тварини власника:"
    ]
    if not pets:
        lines.append("  — немає записів")
    else:
        for i, p in enumerate(pets, 1):
            lines.append(f"  {i}) {p.get('Description','—')} — договір {p.get('НомерДоговора','—')}")
    return "\n".join(lines)

def _kb_confirm(contract_number: str, owner_ref_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Прив’язати", callback_data=f"enl:ok:{contract_number}:{owner_ref_key}")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="enl:cancel")]
    ])

@router.message(Command("enote_link"))
async def cmd_enote_link(message: Message, state: FSMContext):
    # Працюємо тільки в групі/топіку і тільки як reply на повідомлення клієнта
    client_user_id = _topic_client_id_or_none(message)
    if not client_user_id:
        await message.reply("Будь ласка, виконайте команду **/enote_link** у темі клієнта як *reply* на повідомлення клієнта.")
        return
    await state.update_data(client_user_id=client_user_id, actor_id=message.from_user.id, chat_id=message.chat.id, message_id=message.message_id)
    await message.reply("Введіть **номер договору** (як у Єноті):")
    await state.set_state(LinkStates.waiting_contract)

@router.message(LinkStates.waiting_contract, F.text.len() > 0)
async def on_contract_input(message: Message, state: FSMContext):
    data = await state.get_data()
    client_user_id = data["client_user_id"]
    contract = message.text.strip()

    # виклик у Єнот
    try:
        card = enote.odata_get_card_by_contract(contract)
        if not card:
            await message.reply(f"Не знайдено договору **{contract}** у Єноті. Перевірте номер і спробуйте ще.")
            return

        owner_ref = card.get("Хозяин_Key")
        owner_cards = enote.odata_get_owner_cards(owner_ref)
        client = enote.api_get_client(owner_ref)
        owner_name  = enote.extract_owner_name(client) if client else "—"
        owner_phone = enote.extract_owner_phone(client) if client else ""

        # прев’ю
        text = _preview_text(owner_name, owner_phone, owner_ref, owner_cards)
        await state.update_data(contract=contract, owner_ref=owner_ref, owner_name=owner_name, owner_phone=owner_phone)
        await message.reply(text, reply_markup=_kb_confirm(contract, owner_ref))
        await state.set_state(LinkStates.confirming)

        log.info(f'preview | chat={message.chat.id} topic={getattr(message, "message_thread_id", None)} client={client_user_id} contract={contract} owner_ref={owner_ref} owner_phone={owner_phone}')
    except Exception as e:
        log.error(f'error_preview | chat={message.chat.id} client={client_user_id} contract={contract} err={e}')
        await message.reply("Сталася помилка під час звернення до Єнота. Спробуйте пізніше.")

@router.callback_query(LinkStates.confirming, F.data.startswith("enl:"))
async def on_confirm(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    client_user_id = data["client_user_id"]
    actor_id = data["actor_id"]
    chat_id = data["chat_id"]
    contract = data.get("contract")
    owner_ref = data.get("owner_ref")
    owner_name = data.get("owner_name")
    owner_phone = data.get("owner_phone")

    if cb.data == "enl:cancel":
        await cb.message.edit_reply_markup(reply_markup=None)
        await cb.message.answer("Скасовано.")
        await state.clear()
        log.info(f'cancel | chat={chat_id} client={client_user_id} contract={contract}')
        return

        # enl:ok:<contract>:<owner_ref>
    parts = cb.data.split(":", 3)
    if len(parts) >= 4 and parts[1] == "ok":
        conn = None
        try:
            conn = get_conn()
            from core.repositories import clients
            clients.update_enote_link(
                conn,
                telegram_id=client_user_id,
                owner_ref_key=owner_ref,
                owner_name_enote=owner_name,
                owner_phone_enote=owner_phone,
                linked_contract_number=contract,
                linked_by=actor_id
            )
        except Exception as e:
            log.error(f'error_link | chat={chat_id} client={client_user_id} contract={contract} owner_ref={owner_ref} err={e}')
            await cb.message.answer("Не вдалося записати у БД. Спробуйте пізніше.")
            return
        finally:
            if conn is not None:
                conn.close()

        await cb.message.edit_reply_markup(reply_markup=None)
        conf = (
            "✅ Прив’язано власника з Єнота\n"
            f"👤 ПІБ: {owner_name or '—'}\n"
            f"📞 Телефон (Єнот): {owner_phone or '—'}\n"
            f"🔗 owner_ref_key: {owner_ref}\n"
            f"🧾 Договір: {contract}\n"
            f"Виконавець: @{cb.from_user.username or cb.from_user.full_name} (ID {cb.from_user.id})"
        )
        await cb.message.answer(conf)
        log.info(f'linked | chat={chat_id} client={client_user_id} contract={contract} owner_ref={owner_ref} actor={cb.from_user.id}')
        await state.clear()
    else:
        await cb.answer()
