# bot/routers/enote_link.py

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from core.config import settings
from core.db import get_conn
from core.integrations import enote
from core.logging_setup import get_enote_link_logger
from core.repositories import clients as clients_repo
from core.repositories import tickets as repo_t

from bot.utils.staff_guard import IsSupportMember

router = Router(name="enote_link")
log = get_enote_link_logger()


# ===== FSM =====
class LinkStates(StatesGroup):
    wait_contract = State()
    confirming = State()


# ===== helpers =====
def _preview_text(owner_name: str, owner_phone: str, owner_ref: str, pets: list[dict]) -> str:
    lines = [
        f"👤 Власник (Єнот): {owner_name or '—'}",
        f"📞 Телефон (Єнот): <code>{owner_phone or '—'}</code>",
        f"🔗 owner_ref_key: <code>{owner_ref}</code>",
        "🐾 Тварини власника:",
    ]
    if not pets:
        lines.append("  — немає записів")
    else:
        for i, p in enumerate(pets, 1):
            lines.append(
                f"  {i}) {p.get('Description', '—')} — договір {p.get('НомерДоговора', '—')}"
            )
    return "\n".join(lines)


def _kb_confirm(contract_number: str, owner_ref_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Прив’язати",
                    callback_data=f"enl:ok:{contract_number}:{owner_ref_key}",
                ),
                InlineKeyboardButton(text="❌ Скасувати", callback_data="enl:cancel"),
            ]
        ]
    )


async def process_contract_and_preview(message: Message, state: FSMContext, contract: str):
    """Отримує дані з Єнота за номером договору та показує попередній перегляд."""
    data = await state.get_data()
    client_user_id = data.get("client_user_id")

    try:
        card = enote.odata_get_card_by_contract(contract)
        if not card:
            await message.reply(
                f"❌ Не знайдено договору <b>{contract}</b> у Єноті. Перевірте номер і спробуйте ще."
            )
            return

        owner_ref = card.get("Хозяин_Key")
        owner_cards = enote.odata_get_owner_cards(owner_ref)
        client = enote.api_get_client(owner_ref)
        owner_name = enote.extract_owner_name(client) if client else "—"
        owner_phone = enote.extract_owner_phone(client) if client else ""

        # зберігаємо все потрібне для підтвердження
        await state.update_data(
            contract=contract,
            owner_ref=owner_ref,
            owner_name=owner_name,
            owner_phone=owner_phone,
        )

        text = _preview_text(owner_name, owner_phone, owner_ref, owner_cards)
        await message.reply(text, reply_markup=_kb_confirm(contract, owner_ref))
        await state.set_state(LinkStates.confirming)

        log.info(
            "preview | chat=%s topic=%s client=%s contract=%s owner_ref=%s owner_phone=%s",
            message.chat.id,
            getattr(message, "message_thread_id", None),
            client_user_id,
            contract,
            owner_ref,
            owner_phone,
        )
    except Exception as e:
        log.error(
            "error_preview | chat=%s client=%s contract=%s err=%s",
            message.chat.id,
            client_user_id,
            contract,
            e,
        )
        await message.reply("⚠️ Сталася помилка під час звернення до Єнота. Спробуйте пізніше.")


# ===== handlers =====


@router.message(
    Command("enote_link"),
    F.chat.id == settings.support_group_id,
    F.is_topic_message == True,
    IsSupportMember(),
)
async def cmd_enote_link(message: Message, state: FSMContext, bot: Bot):
    # 1) беремо client_user_id із теми
    conn = get_conn()
    try:
        t = repo_t.find_by_thread(conn, message.message_thread_id)
        if not t:
            await message.answer("ℹ️ Не знайшов тікет, прив’язаний до цієї теми.")
            return
        client_user_id = t["client_user_id"]
    finally:
        conn.close()

    # 2) аргумент після команди (дозволяє /enote_link 12345)
    parts = message.text.strip().split(maxsplit=1)
    contract = parts[1].strip() if len(parts) == 2 else None

    # 3) контекст
    await state.update_data(
        chat_id=message.chat.id,
        actor_id=message.from_user.id,
        client_user_id=client_user_id,
    )

    # 4) або одразу працюємо, або запитуємо номер
    if contract:
        await process_contract_and_preview(message, state, contract)
    else:
        await message.answer("Введіть номер договору (як у Єноті):")
        await state.set_state(LinkStates.wait_contract)


@router.message(LinkStates.wait_contract, F.text.len() > 0)
async def on_contract_input(message: Message, state: FSMContext):
    contract = message.text.strip()
    await process_contract_and_preview(message, state, contract)


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
        log.info("cancel | chat=%s client=%s contract=%s", chat_id, client_user_id, contract)
        return

    parts = cb.data.split(":", 3)
    if len(parts) >= 4 and parts[1] == "ok":
        conn = None
        try:
            conn = get_conn()
            clients_repo.update_enote_link(
                conn,
                telegram_id=client_user_id,  # ключ — telegram_id
                owner_ref_key=owner_ref,
                owner_name_enote=owner_name,
                owner_phone_enote=owner_phone,
                linked_contract_number=contract,
                linked_by=actor_id,
            )
        except Exception as e:
            log.error(
                "error_link | chat=%s client=%s contract=%s owner_ref=%s err=%s",
                chat_id,
                client_user_id,
                contract,
                owner_ref,
                e,
            )
            await cb.message.answer("Не вдалося записати у БД. Спробуйте пізніше.")
            return
        finally:
            if conn:
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
        log.info(
            "linked | chat=%s client=%s contract=%s owner_ref=%s actor=%s",
            chat_id,
            client_user_id,
            contract,
            owner_ref,
            cb.from_user.id,
        )
        await state.clear()
    else:
        await cb.answer()
