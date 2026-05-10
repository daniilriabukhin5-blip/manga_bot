import telebot
from telebot import types
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import math
import time
import re
import random
from collections import defaultdict
import threading
import logging
import os
import json

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# 🔑 КОНФИГУРАЦИЯ (секреты берутся из переменных окружения)
TOKEN = os.environ.get('BOT_TOKEN')
if not TOKEN:
    raise ValueError("❌ Переменная окружения BOT_TOKEN не задана!")

bot = telebot.TeleBot(TOKEN)

# 📊 НАСТРОЙКИ
titles_per_page = 10
user_data = {}
user_last_click = {}
favorites_cache = defaultdict(list)
CACHE_TTL = 300

# 🎨 КАТЕГОРИИ С ОПИСАНИЕМ
CATEGORY_INFO = {
    "Фэнтези": {
        "emoji": "🔮",
        "description": "Где магия шепчет в тенях, драконы рисуют закаты крыльями, а судьба решается в один миг между клинками и заклинаниями"
    },
    "Романтика": {
        "emoji": "💞",
        "description": "Тёплые встречи под вишнёвым дождём, сердца, что бьются в унисон сквозь время и расстояния"
    },
    "Мурим": {
        "emoji": "⚔️",
        "description": "Путь воина сквозь туман гор Циминшань — где каждый удар — философия, а честь дороже жизни"
    },
    "Юри": {
        "emoji": "🌸",
        "description": "Нежность, что расцветает тише шёпота ветра, две души, находящие отражение друг в друге"
    },
    "Яой": {
        "emoji": "🌈",
        "description": "Страсть и уязвимость за масками силы, где любовь становится самым опасным и прекрасным боем"
    }
}


# ================== КЭШИРОВАНИЕ ТАЙТЛОВ ==================
class TitlesCache:
    def __init__(self, ttl=CACHE_TTL):
        self.ttl = ttl
        self.cache = []
        self.last_update = 0
        self.lock = threading.Lock()

    def _fetch_from_sheets(self):
        try:
            sheet = connect_to_google_sheets()
            records = sheet.get_all_records()
            cleaned_records = []
            for record in records:
                if not any(record.values()):
                    continue
                cleaned_record = {}
                for key, value in record.items():
                    if value is None:
                        cleaned_record[key] = ''
                    elif isinstance(value, (int, float)):
                        if key in ['Название', 'Автор', 'Категория', 'Статус', 'Тип', 'Описание', 'Теги']:
                            cleaned_record[key] = str(value)
                        else:
                            cleaned_record[key] = value
                    else:
                        cleaned_record[key] = str(value).strip()
                if 'ID' in cleaned_record and cleaned_record['ID']:
                    try:
                        cleaned_record['ID'] = int(str(cleaned_record['ID']))
                        cleaned_records.append(cleaned_record)
                    except:
                        cleaned_records.append(cleaned_record)
            logger.info(f"Загружено {len(cleaned_records)} произведений из Google Sheets")
            return cleaned_records
        except Exception as e:
            logger.error(f"Ошибка загрузки из Google Sheets: {e}")
            return []

    def get(self):
        with self.lock:
            now = time.time()
            if now - self.last_update > self.ttl or not self.cache:
                self.cache = self._fetch_from_sheets()
                self.last_update = now
            return self.cache.copy()

    def invalidate(self):
        with self.lock:
            self.last_update = 0
            logger.info("Кэш тайтлов сброшен")


titles_cache = TitlesCache()


# ================== ФУНКЦИИ ДЛЯ GOOGLE SHEETS ==================
def connect_to_google_sheets():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    # Берём словарь с ключами из переменной окружения
    creds_json = os.environ.get('GOOGLE_CREDS')
    if not creds_json:
        raise ValueError("❌ Переменная окружения GOOGLE_CREDS не задана!")

    try:
        creds_dict = json.loads(creds_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"❌ Ошибка парсинга GOOGLE_CREDS: {e}")

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client.open_by_key("1XKrLkDtaA2jX14WeIqriZGuPmInNcPHn78AGjU4RTSQ").worksheet("Лист1")


def get_all_titles():
    return titles_cache.get()


def get_title_by_id(title_id):
    all_titles = get_all_titles()
    for title in all_titles:
        if str(title.get('ID')) == str(title_id):
            return title
    return None


def get_titles_by_category(category):
    all_titles = get_all_titles()
    filtered = []
    for title in all_titles:
        cat = str(title.get('Категория', '')).lower()
        if cat == category.lower():
            filtered.append(title)
    return filtered


def get_random_title(category=None):
    try:
        all_titles = get_all_titles()
        if not all_titles:
            return None
        if category:
            titles = get_titles_by_category(category)
        else:
            titles = all_titles
        valid_titles = [title for title in titles if title.get('Название', '').strip()]
        if not valid_titles:
            return None
        return random.choice(valid_titles)
    except Exception as e:
        logger.error(f"Ошибка при получении случайного произведения: {e}")
        return None


def get_image_url(title):
    if not title:
        return None
    fields_to_check = ['Картинка', 'Изображение', 'Обложка', 'Image', 'Cover', 'Ссылка на картинку']
    for field in fields_to_check:
        if field in title and title[field]:
            url = str(title[field]).strip()
            if url.startswith('http'):
                return url
    return None


def search_titles(query):
    all_titles = get_all_titles()
    query = query.lower()
    results = []
    for title in all_titles:
        name = str(title.get('Название', '')).lower()
        description = str(title.get('Описание', '')).lower()
        author = str(title.get('Автор', '')).lower()
        if (query in name or query in description or query in author):
            results.append(title)
    return results


# 💖 ИЗБРАННОЕ (с кэшем)
def is_in_favorites(user_id, title_id):
    try:
        if user_id in favorites_cache:
            for fav in favorites_cache[user_id]:
                if str(fav['ID']) == str(title_id):
                    return True
        sheet = connect_to_google_sheets()
        try:
            favorites_sheet = sheet.spreadsheet.worksheet("Избранное")
        except:
            return False
        records = favorites_sheet.get_all_records()
        for record in records:
            if str(record['user_id']) == str(user_id) and str(record['title_id']) == str(title_id):
                return True
        return False
    except Exception as e:
        logger.error(f"Ошибка в is_in_favorites: {e}")
        return False


def add_to_favorites(user_id, title_id):
    try:
        sheet = connect_to_google_sheets()
        try:
            favorites_sheet = sheet.spreadsheet.worksheet("Избранное")
        except:
            favorites_sheet = sheet.spreadsheet.add_worksheet(title="Избранное", rows=1000, cols=3)
            favorites_sheet.append_row(["user_id", "title_id", "date"])
        if is_in_favorites(user_id, title_id):
            return False
        favorites_sheet.append_row([
            str(user_id),
            str(title_id),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ])
        if user_id in favorites_cache:
            del favorites_cache[user_id]
        return True
    except Exception as e:
        logger.error(f"Ошибка в add_to_favorites: {e}")
        return False


def remove_from_favorites(user_id, title_id):
    try:
        sheet = connect_to_google_sheets()
        try:
            favorites_sheet = sheet.spreadsheet.worksheet("Избранное")
        except:
            return False
        all_records = favorites_sheet.get_all_values()
        rows_to_delete = []
        for i in range(len(all_records) - 1, 0, -1):
            row = all_records[i]
            if len(row) >= 2:
                if str(row[0]) == str(user_id) and str(row[1]) == str(title_id):
                    rows_to_delete.append(i + 1)
        if not rows_to_delete:
            return False
        rows_to_delete.sort(reverse=True)
        for row_num in rows_to_delete:
            favorites_sheet.delete_rows(row_num)
        if user_id in favorites_cache:
            del favorites_cache[user_id]
        return True
    except Exception as e:
        logger.error(f"Ошибка в remove_from_favorites: {e}")
        return False


def get_favorites(user_id, use_cache=True):
    try:
        if use_cache and user_id in favorites_cache:
            logger.info(f"Используем кэш избранного для пользователя {user_id}")
            return favorites_cache[user_id].copy()
        sheet = connect_to_google_sheets()
        try:
            favorites_sheet = sheet.spreadsheet.worksheet("Избранное")
        except:
            favorites_cache[user_id] = []
            return []
        records = favorites_sheet.get_all_records()
        user_favorites = []
        all_titles = get_all_titles()
        titles_dict = {str(title.get('ID', '')): title for title in all_titles if title.get('ID')}
        for record in records:
            if str(record['user_id']) == str(user_id):
                title_id_str = str(record['title_id'])
                if title_id_str in titles_dict:
                    user_favorites.append(titles_dict[title_id_str])
        favorites_cache[user_id] = user_favorites.copy()
        logger.info(f"Загружено {len(user_favorites)} избранных для пользователя {user_id}")
        return user_favorites
    except Exception as e:
        logger.error(f"Ошибка в get_favorites: {e}")
        return []


# 🎨 ФУНКЦИИ БОТА
def safe_answer_callback_query(callback_query_id, text=None, show_alert=False):
    try:
        bot.answer_callback_query(callback_query_id, text=text, show_alert=show_alert)
        return True
    except Exception as e:
        if "query is too old" in str(e) or "query ID is invalid" in str(e):
            return False
        else:
            logger.warning(f"Ошибка answer_callback_query: {e}")
            return False


def can_click(user_id):
    current_time = time.time()
    last_click = user_last_click.get(user_id, 0)
    if current_time - last_click < 1:
        return False
    user_last_click[user_id] = current_time
    return True


def format_rating(rating_str):
    try:
        rating = float(rating_str)
        full_stars = int(rating // 2)
        half_star = 1 if rating % 2 >= 1 else 0
        empty_stars = 5 - full_stars - half_star
        stars = "⭐" * full_stars + "🌟" * half_star + "☆" * empty_stars
        return f"{stars} ({rating}/10)"
    except:
        return f"⭐ {rating_str}"


def format_rating_number(rating_str):
    try:
        rating = float(rating_str)
        return f"{rating:.1f}"
    except:
        return rating_str


def format_tags(tags_str):
    if not tags_str:
        return ""
    tags_str = str(tags_str)
    tags = [tag.strip() for tag in tags_str.split(',') if tag.strip()]
    if not tags:
        return ""
    if len(tags) <= 5:
        return f"🏷️ Теги: {' '.join([f'#{tag}' for tag in tags])}"
    else:
        first_five = tags[:5]
        remaining = len(tags) - 5
        return f"🏷️ Теги: {' '.join([f'#{tag}' for tag in first_five])} ... и еще {remaining} тег(ов)"


def create_favorite_button(user_id, title_id):
    if is_in_favorites(user_id, title_id):
        return types.InlineKeyboardButton("💔 Удалить из избранного", callback_data=f"toggle_fav_{title_id}")
    else:
        return types.InlineKeyboardButton("❤️ В избранное", callback_data=f"toggle_fav_{title_id}")


def build_title_keyboard(user_id, title):
    markup = types.InlineKeyboardMarkup(row_width=1)

    if title.get('Ссылка на чтение') and title['Ссылка на чтение'].strip():
        markup.add(types.InlineKeyboardButton("📖 ЧИТАТЬ ОНЛАЙН", url=title['Ссылка на чтение'].strip()))

    markup.add(create_favorite_button(user_id, title['ID']))

    if user_id in user_data and user_data[user_id].get('from_random', False):
        random_type = user_data[user_id].get('random_type', 'absolute')
        if random_type == 'absolute':
            callback_data = "random_absolute"
        elif random_type.startswith('category:'):
            callback_data = f"random_category_{random_type[9:]}"
        else:
            callback_data = "random_absolute"
        markup.add(types.InlineKeyboardButton("🎲 Еще раз", callback_data=callback_data))

    if user_id in user_data:
        if user_data[user_id].get('from_search', False):
            search_page = user_data[user_id].get('search_page', 0)
            markup.add(types.InlineKeyboardButton("🔙 К результатам поиска", callback_data=f"search_page_{search_page}"))
        elif user_data[user_id].get('from_category', False):
            current_category = user_data[user_id].get('current_category')
            if current_category == 'favorites':
                current_page = user_data[user_id].get('current_page', 0)
                markup.add(types.InlineKeyboardButton("🔙 К избранному", callback_data=f"fav_page_{current_page}"))
            elif current_category:
                current_page = user_data[user_id].get('current_page', 0)
                markup.add(types.InlineKeyboardButton(f"🔙 К {current_category}", callback_data=f"page_{current_page}"))
            else:
                markup.add(types.InlineKeyboardButton("🔙 К меню", callback_data="back_to_menu"))
        else:
            markup.add(types.InlineKeyboardButton("🏠 В меню", callback_data="back_to_menu"))
    else:
        markup.add(types.InlineKeyboardButton("🏠 В меню", callback_data="back_to_menu"))
    return markup


def format_manga_description(title):
    name = str(title.get('Название', 'Без названия'))
    category = str(title.get('Категория', ''))
    category_info = CATEGORY_INFO.get(category, {})

    description = f"✨ *{name}*\n"
    description += "─" * 40 + "\n\n"

    if 'Описание' in title and title['Описание']:
        desc = str(title['Описание'])
        description += f"📖 *Описание:*\n{desc}\n\n"

    description += "📋 *Информация:*\n"

    if category:
        emoji = category_info.get('emoji', '📚')
        description += f"{emoji} Категория: {category}\n"

    if 'Автор' in title and title['Автор']:
        description += f"👤 Автор: {str(title['Автор'])}\n"

    if 'Рейтинг' in title and title['Рейтинг']:
        description += f"⭐ Рейтинг: {format_rating(title['Рейтинг'])}\n"

    if 'Год выхода' in title and title['Год выхода']:
        description += f"📅 Год: {str(title['Год выхода'])}\n"

    if 'Статус' in title and title['Статус']:
        status = str(title['Статус'])
        if "онгоинг" in status.lower() or "выходит" in status.lower():
            description += f"🟢 Статус: {status}\n"
        elif "завершен" in status.lower():
            description += f"🔴 Статус: {status}\n"
        else:
            description += f"⚪ Статус: {status}\n"

    tags_text = format_tags(title.get('Теги', ''))
    if tags_text:
        description += f"\n{tags_text}\n"

    manga_type = str(title.get('Тип', ''))
    if manga_type:
        type_emoji = "🇯🇵" if "манга" in manga_type.lower() else "🇰🇷" if "манхва" in manga_type.lower() else "🇨🇳"
        description += f"\n{type_emoji} Тип: {manga_type}"

    return description


# 📱 ГЛАВНОЕ МЕНЮ
@bot.message_handler(commands=['start', 'menu', 'меню'])
def start(message):
    show_main_menu(message.chat.id)
    show_reply_keyboard(message.chat.id)


def show_reply_keyboard(chat_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    btn1 = types.KeyboardButton("📚 MangaLens")
    btn2 = types.KeyboardButton("🎲 Рандом")
    btn3 = types.KeyboardButton("🔍 Поиск")
    markup.row(btn1, btn2, btn3)

    bot.send_message(
        chat_id,
        "🎮 *Используйте кнопки ниже для быстрой навигации:*",
        parse_mode="Markdown",
        reply_markup=markup
    )


def show_main_menu(chat_id, message_id=None):
    all_titles = get_all_titles()
    total_titles = len(all_titles)

    welcome_text = """
༺✦･₊˚  📚 МАНГА-БИБЛИОТЕКА MANGALENS ˚₊･✦༻

*Где каждая страница — портал в новую вселенную*

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🌌 ФОКУС НА ИСТОРИЯХ │ Манга • Манхва • Маньхуа

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✧･̣̣  📖  ВЫБЕРИ СВОЙ МИР  ･̣̣✧

🔮 ФЭНТЕЗИ
*Где магия шепчет в тенях, драконы рисуют закаты крыльями, а судьба решается в один миг между клинками и заклинаниями*

💖 РОМАНТИКА
*Тёплые встречи под вишнёвым дождём, сердца, что бьются в унисон сквозь время и расстояния*

⚔️ МУРИМ
*Путь воина сквозь туман гор Циминшань — где каждый удар — философия, а честь дороже жизни*

🌸 ЮРИ
*Нежность, что расцветает тише шёпота ветра, две души, находящие отражение друг в друге*

🌈 ЯОЙ
*Страсть и уязвимость за масками силы, где любовь становится самым опасным и прекрасным боем*

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💫 СОХРАНЯЙ МОМЕНТЫ В ИЗБРАННОЕ

✨ *Твоя библиотека — твои правила. Здесь каждая история становится частью тебя.*

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

༺  ✧  📖  ✧  🌌  ✧  📖  ✧  ༻

*༺MangaLens༻ — читай сердцем*
"""

    footer = f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📚 *Всего произведений в библиотеке: {total_titles}*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    markup = types.InlineKeyboardMarkup(row_width=2)

    markup.row(
        types.InlineKeyboardButton("🔮 Фэнтези", callback_data="category_Фэнтези"),
        types.InlineKeyboardButton("💞 Романтика", callback_data="category_Романтика")
    )

    markup.add(types.InlineKeyboardButton("⚔️ Мурим", callback_data="category_Мурим"))

    markup.row(
        types.InlineKeyboardButton("🌸 Юри", callback_data="category_Юри"),
        types.InlineKeyboardButton("🌈 Яой", callback_data="category_Яой")
    )

    markup.add(types.InlineKeyboardButton("🎲 Случайное произведение", callback_data="random_menu"))

    markup.row(
        types.InlineKeyboardButton("💝 ИЗБРАННОЕ", callback_data="show_favorites"),
        types.InlineKeyboardButton("🔍 ПОИСК", callback_data="search_manga")
    )

    full_text = welcome_text + footer

    if message_id:
        try:
            bot.edit_message_text(full_text, chat_id, message_id, parse_mode="Markdown", reply_markup=markup)
        except:
            bot.send_message(chat_id, full_text, parse_mode="Markdown", reply_markup=markup)
    else:
        bot.send_message(chat_id, full_text, parse_mode="Markdown", reply_markup=markup)


# 🎲 МЕНЮ РАНДОМА
def show_random_menu(chat_id, message_id=None):
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("🌐 Абсолютный рандом", callback_data="random_absolute"),
        types.InlineKeyboardButton("📚 Рандом по категориям", callback_data="random_category_menu"),
        types.InlineKeyboardButton("🏠 В меню", callback_data="back_to_menu")
    )
    text = "🎲 *Выберите тип случайного произведения:*\n\n" \
           "🌐 *Абсолютный рандом* — любое произведение из всей библиотеки\n" \
           "📚 *Рандом по категориям* — выберите категорию и получите случайное произведение из неё"
    if message_id:
        bot.edit_message_text(text, chat_id, message_id, parse_mode="Markdown", reply_markup=markup)
    else:
        bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=markup)


def show_category_choice_for_random(chat_id, message_id=None):
    markup = types.InlineKeyboardMarkup(row_width=2)
    categories = list(CATEGORY_INFO.keys())
    for category in categories:
        info = CATEGORY_INFO.get(category, {})
        emoji = info.get('emoji', '📚')
        markup.add(types.InlineKeyboardButton(f"{emoji} {category}", callback_data=f"random_category_{category}"))
    markup.add(types.InlineKeyboardButton("🏠 В меню", callback_data="back_to_menu"))
    text = "📚 *Выберите категорию для случайного произведения:*"
    if message_id:
        bot.edit_message_text(text, chat_id, message_id, parse_mode="Markdown", reply_markup=markup)
    else:
        bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=markup)


# 📚 КАТЕГОРИИ (улучшенный дизайн)
def show_category_titles(chat_id, category, page=0, message_id=None):
    try:
        user_id = chat_id
        if user_id not in user_data:
            user_data[user_id] = {}

        titles = get_titles_by_category(category)
        category_info = CATEGORY_INFO.get(category, {})

        if not isinstance(titles, list):
            titles = []

        if not titles:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🏠 В меню", callback_data="back_to_menu"))
            if message_id:
                bot.edit_message_text(f"📭 В этой категории пока нет произведений", chat_id, message_id,
                                      parse_mode="Markdown", reply_markup=markup)
            else:
                bot.send_message(chat_id, f"📭 В этой категории пока нет произведений", parse_mode="Markdown",
                                 reply_markup=markup)
            return

        user_data[user_id]['current_titles'] = titles
        user_data[user_id]['current_category'] = category
        user_data[user_id]['current_page'] = page
        user_data[user_id]['from_category'] = True
        user_data[user_id]['from_search'] = False
        user_data[user_id]['from_random'] = False

        total_pages = max(1, math.ceil(len(titles) / titles_per_page))
        page = min(page, total_pages - 1)

        start_idx = page * titles_per_page
        end_idx = start_idx + titles_per_page
        page_titles = titles[start_idx:end_idx]

        emoji = category_info.get('emoji', '📚')
        description = category_info.get('description', '')

        header = f"{emoji} *{category.upper()}*\n\n"
        header += f"*{description}*\n\n"
        header += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        header += f"📚 Всего произведений: *{len(titles)}*\n"
        header += f"📖 На странице: *{len(page_titles)}* из *{titles_per_page}*\n"
        header += f"🔢 *Страница: {page + 1}/{total_pages}* (нажмите для ввода номера)\n"
        header += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        header += f"✨ *СПИСОК ПРОИЗВЕДЕНИЙ:*\n\n"

        markup = types.InlineKeyboardMarkup()

        for title in page_titles:
            name = str(title.get('Название', ''))
            if len(name) > 25:
                name = name[:22] + "..."

            rating_number = format_rating_number(title.get('Рейтинг', 'N/A'))

            btn_text = f"{emoji} {name} ({rating_number}/10)"
            btn = types.InlineKeyboardButton(btn_text, callback_data=f"title_{title['ID']}")
            markup.add(btn)

        # Навигация: две строки
        # Верхняя строка: начало и конец
        nav_row1 = []
        nav_row1.append(types.InlineKeyboardButton("⏪ Начало", callback_data="page_0"))
        nav_row1.append(types.InlineKeyboardButton("⏩ Конец", callback_data=f"page_{total_pages - 1}"))
        markup.row(*nav_row1)

        # Нижняя строка: назад, индикатор (с вводом), вперёд
        nav_row2 = []
        if page > 0:
            nav_row2.append(types.InlineKeyboardButton("◀️ Назад", callback_data=f"page_{page - 1}"))
        else:
            nav_row2.append(types.InlineKeyboardButton("◀️ Назад", callback_data="no_action"))
        nav_row2.append(types.InlineKeyboardButton(f"🔢 {page + 1}/{total_pages}", callback_data="enter_page_category"))
        if page < total_pages - 1:
            nav_row2.append(types.InlineKeyboardButton("Вперёд ▶️", callback_data=f"page_{page + 1}"))
        else:
            nav_row2.append(types.InlineKeyboardButton("Вперёд ▶️", callback_data="no_action"))
        markup.row(*nav_row2)

        markup.add(types.InlineKeyboardButton("🏠 В меню", callback_data="back_to_menu"))

        if message_id:
            try:
                bot.edit_message_text(header, chat_id, message_id, parse_mode="Markdown", reply_markup=markup)
            except Exception as e:
                msg = bot.send_message(chat_id, header, parse_mode="Markdown", reply_markup=markup)
                user_data[user_id]['last_message_id'] = msg.message_id
        else:
            msg = bot.send_message(chat_id, header, parse_mode="Markdown", reply_markup=markup)
            user_data[user_id]['last_message_id'] = msg.message_id

    except Exception as e:
        logger.error(f"Критическая ошибка в show_category_titles: {e}")
        import traceback
        traceback.print_exc()
        error_markup = types.InlineKeyboardMarkup()
        error_markup.add(types.InlineKeyboardButton("🏠 В меню", callback_data="back_to_menu"))
        error_text = f"❌ Произошла ошибка при загрузке категории '{category}'\n\nПожалуйста, попробуйте позже."
        if message_id:
            try:
                bot.edit_message_text(error_text, chat_id, message_id, parse_mode="Markdown", reply_markup=error_markup)
            except:
                bot.send_message(chat_id, error_text, parse_mode="Markdown", reply_markup=error_markup)
        else:
            bot.send_message(chat_id, error_text, parse_mode="Markdown", reply_markup=error_markup)


# 📖 ПОКАЗАТЬ МАНГУ
def show_title_with_image(call, title, edit_message=False):
    user_id = call.message.chat.id
    description = format_manga_description(title)
    markup = build_title_keyboard(user_id, title)

    image_url = get_image_url(title)

    try:
        if edit_message:
            try:
                bot.delete_message(user_id, call.message.message_id)
            except:
                pass

        if image_url:
            try:
                sent_msg = bot.send_photo(
                    chat_id=user_id,
                    photo=image_url,
                    caption=description,
                    parse_mode="Markdown",
                    reply_markup=markup
                )
            except Exception as photo_err:
                logger.warning(f"Не удалось отправить фото: {photo_err}. Отправляем только текст.")
                sent_msg = bot.send_message(
                    user_id,
                    description,
                    parse_mode="Markdown",
                    reply_markup=markup
                )
        else:
            sent_msg = bot.send_message(
                user_id,
                description,
                parse_mode="Markdown",
                reply_markup=markup
            )
    except Exception as e:
        logger.error(f"Критическая ошибка при показе манги: {e}")
        try:
            bot.send_message(user_id, "❌ Произошла ошибка при загрузке информации.", reply_markup=markup)
        except:
            pass


# 💝 ИЗБРАННОЕ (улучшенный дизайн)
def show_favorites_page(chat_id, page=0, message_id=None):
    user_id = chat_id
    favorites = get_favorites(user_id, use_cache=True)

    if not favorites:
        text = "💝 *ИЗБРАННОЕ*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        text += "✨ *Здесь будут храниться ваши любимые произведения*\n\n"
        text += "💡 *Как добавить?*\n"
        text += "Нажмите ❤️ в описании любого произведения\n\n"
        text += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        text += "📭 *Сейчас избранное пусто*"

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🏠 В меню", callback_data="back_to_menu"))

        if message_id:
            bot.edit_message_text(text, chat_id, message_id, parse_mode="Markdown", reply_markup=markup)
        else:
            bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=markup)
        return

    if user_id not in user_data:
        user_data[user_id] = {}

    user_data[user_id]['current_titles'] = favorites
    user_data[user_id]['current_category'] = 'favorites'
    user_data[user_id]['current_page'] = page
    user_data[user_id]['from_category'] = True
    user_data[user_id]['from_search'] = False
    user_data[user_id]['from_random'] = False

    total_pages = max(1, math.ceil(len(favorites) / titles_per_page))
    page = min(page, total_pages - 1)

    start_idx = page * titles_per_page
    end_idx = start_idx + titles_per_page
    page_titles = favorites[start_idx:end_idx]

    header = "💝 *ИЗБРАННОЕ*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    header += "✨ *Ваша личная коллекция любимых историй*\n\n"
    header += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    header += f"📚 Всего сохранено: *{len(favorites)}*\n"
    header += f"📖 На странице: *{len(page_titles)}* из *{titles_per_page}*\n"
    header += f"🔢 *Страница: {page + 1}/{total_pages}* (нажмите для ввода номера)\n"
    header += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    header += "✨ *СОХРАНЕННЫЕ ПРОИЗВЕДЕНИЯ:*\n\n"

    markup = types.InlineKeyboardMarkup()

    for title in page_titles:
        name = str(title.get('Название', ''))
        if len(name) > 25:
            name = name[:22] + "..."

        category = title.get('Категория', '')
        emoji = CATEGORY_INFO.get(category, {}).get('emoji', '📚')
        rating_number = format_rating_number(title.get('Рейтинг', 'N/A'))

        btn_text = f"{emoji} {name} ({rating_number}/10)"
        markup.row(
            types.InlineKeyboardButton(btn_text, callback_data=f"title_{title['ID']}"),
            types.InlineKeyboardButton("🗑️", callback_data=f"remove_fav_{title['ID']}")
        )

    # Навигация: две строки
    nav_row1 = []
    nav_row1.append(types.InlineKeyboardButton("⏪ Начало", callback_data="fav_page_0"))
    nav_row1.append(types.InlineKeyboardButton("⏩ Конец", callback_data=f"fav_page_{total_pages - 1}"))
    markup.row(*nav_row1)

    nav_row2 = []
    if page > 0:
        nav_row2.append(types.InlineKeyboardButton("◀️ Назад", callback_data=f"fav_page_{page - 1}"))
    else:
        nav_row2.append(types.InlineKeyboardButton("◀️ Назад", callback_data="no_action"))
    nav_row2.append(types.InlineKeyboardButton(f"🔢 {page + 1}/{total_pages}", callback_data="enter_page_fav"))
    if page < total_pages - 1:
        nav_row2.append(types.InlineKeyboardButton("Вперёд ▶️", callback_data=f"fav_page_{page + 1}"))
    else:
        nav_row2.append(types.InlineKeyboardButton("Вперёд ▶️", callback_data="no_action"))
    markup.row(*nav_row2)

    markup.add(types.InlineKeyboardButton("🏠 В МЕНЮ", callback_data="back_to_menu"))

    if message_id:
        try:
            bot.edit_message_text(header, chat_id, message_id, parse_mode="Markdown", reply_markup=markup)
        except Exception as e:
            bot.send_message(chat_id, header, parse_mode="Markdown", reply_markup=markup)
    else:
        bot.send_message(chat_id, header, parse_mode="Markdown", reply_markup=markup)


# 🔍 ПОИСК
def ask_for_search_query(call):
    user_id = call.message.chat.id

    text = "🔍 *ПОИСК ПРОИЗВЕДЕНИЙ*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "✨ *Ищете конкретную мангу, манхву или маньхуа?*\n\n"
    text += "📝 *Как искать:*\n"
    text += "• По названию\n"
    text += "• По автору\n"
    text += "• По ключевым словам\n\n"
    text += "💡 *Примеры:*\n"
    text += "`Сола левеллер`\n"
    text += "`О моём перерождении`\n"
    text += "`Романтика`\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += "📝 *Отправьте название или ключевые слова:*"

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🏠 В меню", callback_data="back_to_menu"))

    try:
        bot.edit_message_text(text, user_id, call.message.message_id, parse_mode="Markdown", reply_markup=markup)
    except:
        bot.send_message(user_id, text, parse_mode="Markdown", reply_markup=markup)

    if user_id not in user_data:
        user_data[user_id] = {}
    user_data[user_id]['waiting_for_search'] = True


def show_search_results(chat_id, query, page=0, message_id=None):
    user_id = chat_id
    results = search_titles(query)

    if not results:
        text = f"🔍 *РЕЗУЛЬТАТЫ ПОИСКА: '{query}'*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        text += "📭 *Ничего не найдено*\n\n"
        text += "💡 *Советы по поиску:*\n"
        text += "• Проверьте правильность написания\n"
        text += "• Попробуйте более короткий запрос\n"
        text += "• Используйте ключевые слова\n\n"
        text += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔍 Новый поиск", callback_data="search_manga"))
        markup.add(types.InlineKeyboardButton("🏠 В меню", callback_data="back_to_menu"))

        if message_id:
            bot.edit_message_text(text, chat_id, message_id, parse_mode="Markdown", reply_markup=markup)
        else:
            bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=markup)
        return

    if user_id not in user_data:
        user_data[user_id] = {}

    user_data[user_id]['search_results'] = results
    user_data[user_id]['search_query'] = query
    user_data[user_id]['search_page'] = page
    user_data[user_id]['from_search'] = True
    user_data[user_id]['from_category'] = False
    user_data[user_id]['from_random'] = False

    total_pages = max(1, math.ceil(len(results) / titles_per_page))
    page = min(page, total_pages - 1)

    start_idx = page * titles_per_page
    end_idx = start_idx + titles_per_page
    page_titles = results[start_idx:end_idx]

    text = f"🔍 *РЕЗУЛЬТАТЫ ПОИСКА: '{query}'*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    text += f"📚 Найдено произведений: *{len(results)}*\n"
    text += f"📖 На странице: *{len(page_titles)}* из *{titles_per_page}*\n"
    text += f"📄 Страница: *{page + 1}/{total_pages}*\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "✨ *НАЙДЕННЫЕ ПРОИЗВЕДЕНИЯ:*\n\n"

    markup = types.InlineKeyboardMarkup()

    for title in page_titles:
        name = str(title.get('Название', ''))
        if len(name) > 25:
            name = name[:22] + "..."

        category = title.get('Категория', '')
        emoji = CATEGORY_INFO.get(category, {}).get('emoji', '📚')
        rating_number = format_rating_number(title.get('Рейтинг', 'N/A'))

        btn_text = f"{emoji} {name} ({rating_number}/10)"
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f"title_{title['ID']}"))

    nav_buttons = []
    if page > 0:
        nav_buttons.append(types.InlineKeyboardButton("◀️ Назад", callback_data=f"search_page_{page - 1}"))
    nav_buttons.append(types.InlineKeyboardButton(f"🔍 {page + 1}/{total_pages}", callback_data="no_action"))
    if page < total_pages - 1:
        nav_buttons.append(types.InlineKeyboardButton("Вперёд ▶️", callback_data=f"search_page_{page + 1}"))
    if nav_buttons:
        markup.row(*nav_buttons)

    markup.row(
        types.InlineKeyboardButton("🔍 Новый поиск", callback_data="search_manga"),
        types.InlineKeyboardButton("🏠 В меню", callback_data="back_to_menu")
    )

    if message_id:
        try:
            bot.edit_message_text(text, chat_id, message_id, parse_mode="Markdown", reply_markup=markup)
        except:
            bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=markup)
    else:
        bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=markup)


# 🎮 ОБРАБОТЧИК КНОПОК
@bot.callback_query_handler(func=lambda call: True)
def callback(call):
    user_id = call.message.chat.id

    if not can_click(user_id):
        safe_answer_callback_query(call.id, "⏳ Пожалуйста, подождите 1 секунду между действиями", show_alert=False)
        return

    # Рандом меню
    if call.data == "random_menu":
        safe_answer_callback_query(call.id)
        show_random_menu(user_id, call.message.message_id)

    elif call.data == "random_absolute":
        safe_answer_callback_query(call.id, "🎲 Ищу случайное произведение...")
        random_title = get_random_title()
        if random_title:
            if user_id not in user_data:
                user_data[user_id] = {}
            user_data[user_id]['from_random'] = True
            user_data[user_id]['random_type'] = 'absolute'
            user_data[user_id]['from_search'] = False
            user_data[user_id]['from_category'] = False
            show_title_with_image(call, random_title, edit_message=True)
        else:
            safe_answer_callback_query(call.id, "❌ Не удалось найти случайное произведение", show_alert=True)
            show_main_menu(user_id, call.message.message_id)

    elif call.data == "random_category_menu":
        safe_answer_callback_query(call.id)
        show_category_choice_for_random(user_id, call.message.message_id)

    elif call.data.startswith("random_category_"):
        category = call.data.replace("random_category_", "")
        safe_answer_callback_query(call.id, f"🎲 Ищу случайное произведение в категории {category}...")
        random_title = get_random_title(category)
        if random_title:
            if user_id not in user_data:
                user_data[user_id] = {}
            user_data[user_id]['from_random'] = True
            user_data[user_id]['random_type'] = f'category:{category}'
            user_data[user_id]['from_search'] = False
            user_data[user_id]['from_category'] = False
            show_title_with_image(call, random_title, edit_message=True)
        else:
            safe_answer_callback_query(call.id, f"❌ В категории {category} нет произведений", show_alert=True)
            show_category_choice_for_random(user_id, call.message.message_id)

    # Избранное: toggle без перезагрузки
    elif call.data.startswith("toggle_fav_"):
        title_id = call.data.replace("toggle_fav_", "")
        title = get_title_by_id(title_id)
        if not title:
            safe_answer_callback_query(call.id, "❌ Произведение не найдено")
            return

        if is_in_favorites(user_id, title_id):
            if remove_from_favorites(user_id, title_id):
                safe_answer_callback_query(call.id, "💔 Удалено из избранного")
                new_markup = build_title_keyboard(user_id, title)
                try:
                    bot.edit_message_reply_markup(chat_id=user_id, message_id=call.message.message_id,
                                                  reply_markup=new_markup)
                except Exception as e:
                    logger.warning(f"Не удалось обновить клавиатуру: {e}")
        else:
            if add_to_favorites(user_id, title_id):
                safe_answer_callback_query(call.id, "❤️ Добавлено в избранное")
                new_markup = build_title_keyboard(user_id, title)
                try:
                    bot.edit_message_reply_markup(chat_id=user_id, message_id=call.message.message_id,
                                                  reply_markup=new_markup)
                except Exception as e:
                    logger.warning(f"Не удалось обновить клавиатуру: {e}")

    elif call.data.startswith("remove_fav_"):
        title_id = call.data.replace("remove_fav_", "")

        if remove_from_favorites(user_id, title_id):
            safe_answer_callback_query(call.id, "🗑️ Удалено из избранного")

            current_page = user_data.get(user_id, {}).get('current_page', 0)
            favorites = get_favorites(user_id, use_cache=False)

            if not favorites:
                show_favorites_page(user_id, 0, call.message.message_id)
            else:
                user_data[user_id]['current_titles'] = favorites
                total_pages = max(1, math.ceil(len(favorites) / titles_per_page))
                if current_page >= total_pages:
                    current_page = max(0, total_pages - 1)
                    user_data[user_id]['current_page'] = current_page
                show_favorites_page(user_id, current_page, call.message.message_id)

    elif call.data.startswith("category_"):
        category = call.data.replace("category_", "")
        if user_id not in user_data:
            user_data[user_id] = {}
        user_data[user_id]['from_category'] = True
        user_data[user_id]['from_random'] = False
        user_data[user_id]['from_search'] = False
        show_category_titles(user_id, category, 0, call.message.message_id)

    elif call.data.startswith("title_"):
        title_id = call.data.replace("title_", "")
        title = get_title_by_id(title_id)

        if title:
            show_title_with_image(call, title, edit_message=True)

    elif call.data == "show_favorites":
        if user_id not in user_data:
            user_data[user_id] = {}
        user_data[user_id]['from_category'] = True
        user_data[user_id]['from_random'] = False
        user_data[user_id]['from_search'] = False
        show_favorites_page(user_id, 0, call.message.message_id)

    elif call.data == "search_manga":
        if user_id not in user_data:
            user_data[user_id] = {}
        user_data[user_id]['from_search'] = True
        user_data[user_id]['from_random'] = False
        user_data[user_id]['from_category'] = False
        ask_for_search_query(call)

    elif call.data.startswith("page_"):
        page = int(call.data.replace("page_", ""))
        if user_id in user_data and 'current_category' in user_data[user_id]:
            current_category = user_data[user_id]['current_category']
            user_data[user_id]['current_page'] = page
            show_category_titles(user_id, current_category, page, call.message.message_id)

    elif call.data.startswith("fav_page_"):
        page = int(call.data.replace("fav_page_", ""))
        show_favorites_page(user_id, page, call.message.message_id)

    elif call.data.startswith("search_page_"):
        page = int(call.data.replace("search_page_", ""))
        if user_id in user_data and 'search_query' in user_data[user_id]:
            query = user_data[user_id]['search_query']
            user_data[user_id]['search_page'] = page
            show_search_results(user_id, query, page, call.message.message_id)

    # Ввод номера страницы
    elif call.data == "enter_page_category":
        user_data[user_id]['target_message_id'] = call.message.message_id
        total_pages = math.ceil(len(user_data[user_id].get('current_titles', [])) / titles_per_page)
        user_data[user_id]['waiting_for_page'] = 'category'
        bot.send_message(user_id, f"🔢 *Введите номер страницы (от 1 до {total_pages}):*", parse_mode="Markdown")
        safe_answer_callback_query(call.id)

    elif call.data == "enter_page_fav":
        user_data[user_id]['target_message_id'] = call.message.message_id
        favorites = get_favorites(user_id)
        total_pages = max(1, math.ceil(len(favorites) / titles_per_page))
        user_data[user_id]['waiting_for_page'] = 'favorites'
        bot.send_message(user_id, f"🔢 *Введите номер страницы (от 1 до {total_pages}):*", parse_mode="Markdown")
        safe_answer_callback_query(call.id)

    elif call.data == "back_to_menu":
        if user_id in user_data:
            user_data[user_id]['from_random'] = False
            user_data[user_id]['from_search'] = False
            user_data[user_id]['from_category'] = False
        show_main_menu(user_id, call.message.message_id)

    elif call.data == "no_action":
        safe_answer_callback_query(call.id)

    else:
        safe_answer_callback_query(call.id)


# 📝 ОБРАБОТЧИК ТЕКСТОВЫХ СООБЩЕНИЙ
@bot.message_handler(func=lambda message: True)
def handle_text(message):
    user_id = message.chat.id
    text = message.text.strip()

    # Обработка ввода номера страницы
    if user_id in user_data and 'waiting_for_page' in user_data[user_id]:
        mode = user_data[user_id]['waiting_for_page']
        try:
            page_num = int(text) - 1
            target_message_id = user_data[user_id].get('target_message_id')
            if mode == 'category':
                titles = user_data[user_id].get('current_titles', [])
                total_pages = math.ceil(len(titles) / titles_per_page)
                if 0 <= page_num < total_pages:
                    del user_data[user_id]['waiting_for_page']
                    current_category = user_data[user_id]['current_category']
                    bot.delete_message(user_id, message.message_id)
                    show_category_titles(user_id, current_category, page_num, target_message_id)
                else:
                    bot.send_message(user_id, f"❌ *Неверный номер. Введите число от 1 до {total_pages}.*",
                                     parse_mode="Markdown")
            elif mode == 'favorites':
                favorites = get_favorites(user_id)
                total_pages = max(1, math.ceil(len(favorites) / titles_per_page))
                if 0 <= page_num < total_pages:
                    del user_data[user_id]['waiting_for_page']
                    bot.delete_message(user_id, message.message_id)
                    show_favorites_page(user_id, page_num, target_message_id)
                else:
                    bot.send_message(user_id, f"❌ *Неверный номер. Введите число от 1 до {total_pages}.*",
                                     parse_mode="Markdown")
        except ValueError:
            bot.send_message(user_id, "❌ *Пожалуйста, введите число.*", parse_mode="Markdown")
        return

    # Обработка поиска
    if user_id in user_data and user_data[user_id].get('waiting_for_search', False):
        query = text
        if len(query) < 2:
            bot.send_message(user_id, "🔍 *Запрос слишком короткий. Введите минимум 2 символа.*", parse_mode="Markdown")
            return
        user_data[user_id]['waiting_for_search'] = False
        user_data[user_id]['from_search'] = True
        user_data[user_id]['from_random'] = False
        user_data[user_id]['from_category'] = False
        show_search_results(user_id, query)

    elif text == "📚 MangaLens":
        show_main_menu(user_id)
        show_reply_keyboard(user_id)

    elif text == "🎲 Рандом":
        show_random_menu(user_id)

    elif text == "🔍 Поиск":
        if user_id not in user_data:
            user_data[user_id] = {}
        user_data[user_id]['waiting_for_search'] = True
        user_data[user_id]['from_search'] = True
        user_data[user_id]['from_random'] = False
        user_data[user_id]['from_category'] = False

        bot.send_message(
            user_id,
            "🔍 *Введите название, автора или ключевые слова для поиска:*",
            parse_mode="Markdown"
        )

    elif message.text.startswith('/'):
        if message.text.startswith('/start') or message.text.startswith('/menu') or message.text.startswith('/меню'):
            show_main_menu(user_id)
            show_reply_keyboard(user_id)
    else:
        bot.send_message(user_id,
                         "ℹ️ Используйте кнопки меню для навигации. Для поиска нажмите 🔍 ПОИСК.")


# 🚀 ЗАПУСК БОТА
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("✨ МАНГА-БОТ MANGALENS ЗАПУЩЕН")
    logger.info("=" * 60)

    titles = get_all_titles()
    logger.info(f"✅ Загружено произведений: {len(titles)}")

    if titles:
        categories_count = {}
        for title in titles:
            category = title.get('Категория', 'Без категории')
            categories_count[category] = categories_count.get(category, 0) + 1
        logger.info("📂 Категории:")
        for category, count in categories_count.items():
            emoji = CATEGORY_INFO.get(category, {}).get('emoji', '📁')
            logger.info(f"  {emoji} {category}: {count}")

    logger.info("=" * 60)
    logger.info("🎮 Бот готов к работе!")
    logger.info("🏠 Главное меню: /start или нажмите '📚 MangaLens'")
    logger.info("🎲 Рандом: нажмите '🎲 Рандом' для выбора типа")
    logger.info("🔍 Поиск: нажмите '🔍 Поиск'")
    logger.info("⏳ Задержка между кликами: 1 секунда")
    logger.info("=" * 60)

    # Используем infinity_polling для стабильной работы 24/7
    try:
        bot.infinity_polling(skip_pending=True)
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        import traceback

        traceback.print_exc()