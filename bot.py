import pandas as pd
from datetime import datetime, timedelta
import logging
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
import asyncio
import os
import json
import secrets
from openpyxl import load_workbook


# Настройки логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Глобальные переменные
CHAT_IDS_FILE = 'chat_ids.json'
AUTHORIZED_USERS_FILE = 'authorized_users.json'
TOKEN = secrets.TOKEN  # Замените на токен вашего бота
TIMEZONE = 'Europe/Moscow'
FILE_INFO_FILE = 'file_info.xlsx'

# Глобальный объект bot и глобальный цикл событий
bot = Bot(token=TOKEN)
loop = asyncio.get_event_loop()


def load_chat_ids():
    if os.path.exists(CHAT_IDS_FILE):
        with open(CHAT_IDS_FILE, 'r') as file:
            return json.load(file)
    return []


def save_chat_ids(chat_ids):
    with open(CHAT_IDS_FILE, 'w') as file:
        json.dump(chat_ids, file)


def load_authorized_users():
    if os.path.exists(AUTHORIZED_USERS_FILE):
        with open(AUTHORIZED_USERS_FILE, 'r') as file:
            return json.load(file)
    return []


def save_authorized_users(user_ids):
    with open(AUTHORIZED_USERS_FILE, 'w') as file:
        json.dump(user_ids, file)


def load_file_info():
    if os.path.exists(FILE_INFO_FILE):
        df = pd.read_excel(FILE_INFO_FILE, engine='openpyxl')
        return {
            'FILE_PATH': df.loc[0, 'FILE_PATH'],
            'SHEET_NAME': df.loc[0, 'SHEET_NAME']
        }
    return {'FILE_PATH': '', 'SHEET_NAME': 'Дежурства 2024'}


def save_file_info(file_info):
    df = pd.DataFrame([file_info])
    df.to_excel(FILE_INFO_FILE, index=False, engine='openpyxl')


# Глобальная переменная для хранения ID чатов и информации о файле
CHAT_IDS = load_chat_ids()
FILE_INFO = load_file_info()
FILE_PATH = FILE_INFO['FILE_PATH']
SHEET_NAME = FILE_INFO['SHEET_NAME']


def check_and_add_chat_id(chat_id, chat_type):
    if chat_type in ['group', 'supergroup']:
        if chat_id not in CHAT_IDS:
            CHAT_IDS.append(chat_id)
            save_chat_ids(CHAT_IDS)
            logger.info(f"Чат ID {chat_id} добавлен.")
        else:
            logger.info(f"Чат ID {chat_id} уже существует.")
    else:
        logger.info(f"Чат ID {chat_id} не добавлен, так как это не группа.")


def load_excel(file_path, sheet_name='Дежурства 2024'):
    df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
    df.columns = df.columns.str.strip()  # Удаление пробелов из названий столбцов
    return df


def get_duty_for_date(df, target_date):
    target_date_str = target_date.strftime('%Y-%m-%d')
    df['Дата'] = pd.to_datetime(df['Дата'], errors='coerce').dt.strftime('%Y-%m-%d')
    target_df = df[df['Дата'] == target_date_str]
    if target_df.empty:
        return f"Нет данных о дежурных на {target_date_str}."
    messages = []
    for _, row in target_df.iterrows():
        date_str = row['Дата']
        time_str = row['Время']
        for column in row.index[3:]:
            if not pd.isna(row[column]):
                duty_info = f"Время: {time_str} - {column.split(' ')[0]}"
                messages.append(duty_info)
    return "\n".join(messages) if messages else f"Нет дежурных на {target_date_str}."


async def send_duties():
    logger.info("Отправка дежурств началась")
    try:
        df = load_excel(FILE_PATH, SHEET_NAME)
        today = datetime.now(pytz.timezone(TIMEZONE))
        tomorrow = today + timedelta(days=1)
        today_duty = get_duty_for_date(df, today)
        tomorrow_duty = get_duty_for_date(df, tomorrow)
        message = f"Дежурные на сегодня:\n{today_duty}\n\nДежурные на завтра:\n{tomorrow_duty}"
        for chat_id in CHAT_IDS:
            await bot.send_message(chat_id=chat_id, text=message)
        logger.info("Сообщение отправлено")
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения: {e}")


async def duties(update: Update, context: CallbackContext) -> None:
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    check_and_add_chat_id(chat_id, chat_type)
    df = load_excel(FILE_PATH, SHEET_NAME)
    today = datetime.now(pytz.timezone(TIMEZONE))
    tomorrow = today + timedelta(days=1)
    today_duty = get_duty_for_date(df, today)
    tomorrow_duty = get_duty_for_date(df, tomorrow)
    message = f"Дежурные на сегодня:\n{today_duty}\n\nДежурные на завтра:\n{tomorrow_duty}"
    await update.message.reply_text(message)


async def duties_week(update: Update, context: CallbackContext) -> None:
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    check_and_add_chat_id(chat_id, chat_type)
    df = load_excel(FILE_PATH, SHEET_NAME)
    today = datetime.now(pytz.timezone(TIMEZONE))
    message = "Расписание на неделю:\n"
    for single_date in (today + timedelta(days=n) for n in range(8)):
        duties = get_duty_for_date(df, single_date)
        message += f"\n{single_date.strftime('%Y-%m-%d')}\n{duties}\n"
    await update.message.reply_text(message)


async def duties_month(update: Update, context: CallbackContext) -> None:
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    check_and_add_chat_id(chat_id, chat_type)
    df = load_excel(FILE_PATH, SHEET_NAME)
    today = datetime.now(pytz.timezone(TIMEZONE))
    message = "Расписание на месяц:\n"
    for single_date in (today + timedelta(days=n) for n in range(31)):
        duties = get_duty_for_date(df, single_date)
        message += f"\n{single_date.strftime('%Y-%m-%d')}:\n{duties}\n"
    await update.message.reply_text(message)


async def findnext(update: Update, context: CallbackContext) -> None:
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    check_and_add_chat_id(chat_id, chat_type)
    if not context.args:
        await update.message.reply_text('Пожалуйста, укажите фамилию дежурного.')
        return

    last_name = context.args[0].strip()
    df = load_excel(FILE_PATH, SHEET_NAME)
    df['Дата'] = pd.to_datetime(df['Дата'], errors='coerce', format='%d %b %a')
    df.dropna(subset=['Дата'], inplace=True)
    df['Дата'] = df['Дата'].dt.tz_localize(TIMEZONE, ambiguous='NaT', nonexistent='shift_forward')
    today = datetime.now(pytz.timezone(TIMEZONE))
    df = df[df['Дата'] > today]
    name_column = None
    for column in df.columns[3:]:
        if last_name in column:
            name_column = column
            break
    if name_column is None:
        await update.message.reply_text(f"Не найдено столбца для фамилии {last_name}.")
        return
    df = df[df[name_column].notna()]
    if df.empty:
        await update.message.reply_text(f"Нет дежурств для {last_name} в будущем.")
        return
    next_duty = df.iloc[0]
    date_str = next_duty['Дата'].strftime('%d %b %a')
    time_str = next_duty['Время']
    await update.message.reply_text(f"Следующее дежурство {last_name}:\n{date_str}, Время: {time_str}")


async def start(update: Update, context: CallbackContext) -> None:
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    check_and_add_chat_id(chat_id, chat_type)
    await update.message.reply_text('''#Обновить файл (нужно отправить файл с этокй командой одним сообщением)
/updatefile
Получить расписание
/duties
/duties_week
/duties_month
/findnext
Пример: /findnext Иванов
/getid''')


async def update_file(update: Update, context: CallbackContext) -> None:
    logger.info("Функция update_file вызвана")
    user_id = update.effective_user.id
    authorized_users = load_authorized_users()
    logger.info(f"Авторизованные пользователи: {authorized_users}")

    if user_id not in authorized_users:
        logger.info(f"Пользователь ID {user_id} не авторизован.")
        await update.message.reply_text('У вас нет прав на обновление файла.')
        return

    if context.args:
        global FILE_PATH
        if os.path.exists(FILE_PATH):
            os.remove(FILE_PATH)
        FILE_PATH = context.args[0]
        save_file_info({'FILE_PATH': FILE_PATH, 'SHEET_NAME': SHEET_NAME})
        await update.message.reply_text(f'Путь к файлу обновлен: {FILE_PATH}')
    else:
        await update.message.reply_text('Пожалуйста, укажите путь к новому файлу.')


async def handle_document(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    authorized_users = load_authorized_users()

    if user_id not in authorized_users:
        await update.message.reply_text('У вас нет прав на загрузку файла.')
        return

    document = update.message.document
    file_name = document.file_name
    if file_name.endswith('.xlsx'):
        file = await context.bot.get_file(document.file_id)
        file_path = os.path.join(os.getcwd(), file_name)
        await file.download_to_drive(file_path)
        global FILE_PATH
        FILE_PATH = file_path
        save_file_info({'FILE_PATH': FILE_PATH, 'SHEET_NAME': SHEET_NAME})
        await update.message.reply_text(f'Файл {file_name} получен и обновлен.')
    else:
        await update.message.reply_text('Пожалуйста, отправьте файл в формате .xlsx.')


async def add_user(update: Update, context: CallbackContext) -> None:
    admin_id = update.effective_user.id
    authorized_users = load_authorized_users()
    # Проверка на администраторов не добавлена в текущем коде

    if len(context.args) != 1:
        await update.message.reply_text('Пожалуйста, укажите ID пользователя для добавления.')
        return

    try:
        new_user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text('Неверный формат ID пользователя.')
        return

    if new_user_id not in authorized_users:
        authorized_users.append(new_user_id)
        save_authorized_users(authorized_users)
        await update.message.reply_text(f'Пользователь с ID {new_user_id} добавлен в список авторизованных.')
    else:
        await update.message.reply_text(f'Пользователь с ID {new_user_id} уже в списке авторизованных.')


def schedule_send_duties(scheduler: BackgroundScheduler):
    scheduler.add_job(
        lambda: asyncio.run_coroutine_threadsafe(send_duties(), loop),
        CronTrigger(hour=7, minute=0, timezone=TIMEZONE),
    )


async def get_user_id(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    await update.message.reply_text(f'Ваш ID: {user_id}')


def main():
    global bot
    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("updatefile", update_file))
    application.add_handler(CommandHandler("duties", duties))
    application.add_handler(CommandHandler("duties_week", duties_week))
    application.add_handler(CommandHandler("duties_month", duties_month))
    application.add_handler(CommandHandler("findnext", findnext))
    application.add_handler(CommandHandler("adduser", add_user))
    application.add_handler(CommandHandler("getid", get_user_id))
    application.add_handler(MessageHandler(filters.Document.FileExtension("xlsx"), handle_document))

    scheduler = BackgroundScheduler(timezone=TIMEZONE)
    schedule_send_duties(scheduler)
    logger.info("Запуск планировщика задач")
    scheduler.start()
    application.run_polling()


if __name__ == '__main__':
    main()
