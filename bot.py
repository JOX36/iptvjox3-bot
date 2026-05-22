# -*- coding: utf-8 -*-
import os
import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters
import re
import socket
import requests
import urllib.parse
from datetime import datetime
import logging
import asyncio
import math
from cachetools import TTLCache
from tenacity import retry, stop_after_attempt, wait_exponential
from io import BytesIO
import difflib
import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = os.environ.get("BOT_TOKEN")

cache = TTLCache(maxsize=100, ttl=300)

_RE_HTML_TAGS = re.compile(r'<[^>]+>')
def strip_html(text: str) -> str:
    return _RE_HTML_TAGS.sub('', text)

def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def ensure_chat_data(context, chat_id):
    context.bot_data.setdefault('chat_data', {})
    context.bot_data['chat_data'].setdefault(chat_id, {
        'custom_list': [],
        'pending_messages': [],
        'start_messages': [],
        'check_messages': [],
        'awaiting_search': False,
        'awaiting_add': False,
        'awaiting_category': False,
        'awaiting_check_url': False,
        'awaiting_compare_url1': False,
        'awaiting_compare_url2': False,
        'compare_url1': None,
        'awaiting_fusion_url': False,
        'fusion_urls': [],
        'fusion_tipos': [],
        'awaiting_fusion_tipos': False,
        'selected_category': None,
        'm3u_url': None,
        'm3u_url_for_info': None,
    })

TIMEZONE_PAIS = {
    "Africa/Algiers": "DZ", "Africa/Cairo": "EG", "Africa/Casablanca": "MA",
    "Africa/Johannesburg": "ZA", "Africa/Lagos": "NG", "Africa/Nairobi": "KE",
    "Africa/Tunis": "TN", "America/Anchorage": "US", "America/Argentina/Buenos_Aires": "AR",
    "America/Bogota": "CO", "America/Caracas": "VE", "America/Chicago": "US",
    "America/Denver": "US", "America/Guayaquil": "EC", "America/Havana": "CU",
    "America/Lima": "PE", "America/Los_Angeles": "US", "America/Mexico_City": "MX",
    "America/Miami": "US", "America/Monterrey": "MX", "America/New_York": "US",
    "America/Panama": "PA", "America/Phoenix": "US", "America/Puerto_Rico": "PR",
    "America/Santiago": "CL", "America/Sao_Paulo": "BR", "America/Toronto": "CA",
    "America/Vancouver": "CA", "Asia/Almaty": "KZ", "Asia/Amman": "JO",
    "Asia/Baghdad": "IQ", "Asia/Baku": "AZ", "Asia/Bangkok": "TH",
    "Asia/Beirut": "LB", "Asia/Colombo": "LK", "Asia/Damascus": "SY",
    "Asia/Dhaka": "BD", "Asia/Dubai": "AE", "Asia/Ho_Chi_Minh": "VN",
    "Asia/Hong_Kong": "HK", "Asia/Istanbul": "TR", "Asia/Jakarta": "ID",
    "Asia/Jerusalem": "IL", "Asia/Karachi": "PK", "Asia/Kathmandu": "NP",
    "Asia/Kolkata": "IN", "Asia/Kuala_Lumpur": "MY", "Asia/Kuwait": "KW",
    "Asia/Manila": "PH", "Asia/Muscat": "OM", "Asia/Nicosia": "CY",
    "Asia/Qatar": "QA", "Asia/Riyadh": "SA", "Asia/Seoul": "KR",
    "Asia/Shanghai": "CN", "Asia/Singapore": "SG", "Asia/Taipei": "TW",
    "Asia/Tehran": "IR", "Asia/Tokyo": "JP", "Asia/Tashkent": "UZ",
    "Asia/Tbilisi": "GE", "Asia/Yerevan": "AM", "Atlantic/Reykjavik": "IS",
    "Australia/Adelaide": "AU", "Australia/Brisbane": "AU", "Australia/Melbourne": "AU",
    "Australia/Perth": "AU", "Australia/Sydney": "AU",
    "Europe/Amsterdam": "NL", "Europe/Athens": "GR", "Europe/Belgrade": "RS",
    "Europe/Berlin": "DE", "Europe/Brussels": "BE", "Europe/Bucharest": "RO",
    "Europe/Budapest": "HU", "Europe/Dublin": "IE", "Europe/Helsinki": "FI",
    "Europe/Istanbul": "TR", "Europe/Kiev": "UA", "Europe/Kyiv": "UA",
    "Europe/Lisbon": "PT", "Europe/London": "GB", "Europe/Luxembourg": "LU",
    "Europe/Madrid": "ES", "Europe/Minsk": "BY", "Europe/Moscow": "RU",
    "Europe/Oslo": "NO", "Europe/Paris": "FR", "Europe/Prague": "CZ",
    "Europe/Riga": "LV", "Europe/Rome": "IT", "Europe/Sofia": "BG",
    "Europe/Stockholm": "SE", "Europe/Tallinn": "EE", "Europe/Vienna": "AT",
    "Europe/Vilnius": "LT", "Europe/Warsaw": "PL", "Europe/Zagreb": "HR",
    "Europe/Zurich": "CH", "Pacific/Auckland": "NZ", "Pacific/Honolulu": "US",
    "Pacific/Fiji": "FJ", "US/Eastern": "US", "US/Central": "US",
    "US/Mountain": "US", "US/Pacific": "US",
}

def codigo_a_bandera(codigo: str) -> str:
    if not codigo or len(codigo) != 2:
        return ""
    try:
        return chr(0x1F1E6 + ord(codigo[0].upper()) - ord('A')) + \
               chr(0x1F1E6 + ord(codigo[1].upper()) - ord('A'))
    except Exception:
        return ""

def timezone_a_bandera(tz: str) -> str:
    if not tz or tz in ("No disponible", "Desconocida", ""):
        return ""
    codigo = TIMEZONE_PAIS.get(tz)
    if not codigo:
        for key, val in TIMEZONE_PAIS.items():
            if tz in key or key in tz:
                codigo = val
                break
    return codigo_a_bandera(codigo) if codigo else ""

async def geolocate_host(hostname: str) -> tuple:
    try:
        loop = asyncio.get_event_loop()
        ip = await loop.run_in_executor(None, socket.gethostbyname, hostname)
        data = await async_get(
            f"http://ip-api.com/json/{ip}?fields=status,timezone,city,country,countryCode,isp"
        )
        if isinstance(data, dict) and data.get("status") == "success":
            return (
                data.get("timezone", ""),
                data.get("city", ""),
                data.get("country", ""),
                data.get("countryCode", ""),
                data.get("isp", ""),
                ip,
            )
    except Exception:
        pass
    return ("", "", "", "", "", "")

def resolver_ip_m3u(host: str, username: str, password: str) -> str:
    try:
        parsed = urllib.parse.urlparse(host)
        domain = parsed.hostname or ""
        ip = socket.gethostbyname(domain)
        if ip and ip != domain:
            proto = "https://" if host.startswith("https://") else "http://"
            port_str = f":{parsed.port}" if parsed.port else ""
            return f"{proto}{ip}{port_str}/get.php?username={username}&password={password}&type=m3u_plus"
    except Exception:
        pass
    return ""

def safe_log_url(url):
    parsed = urllib.parse.urlparse(url)
    query_params = urllib.parse.parse_qs(parsed.query)
    safe_query = {k: ['*****'] if k in ['username', 'password'] else v for k, v in query_params.items()}
    safe_url = parsed._replace(query=urllib.parse.urlencode(safe_query, doseq=True))
    return safe_url.geturl()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def safe_get(url):
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return response

async def async_get(url, retries=3):
    timeout = aiohttp.ClientTimeout(total=15)
    last_exc = None
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for attempt in range(retries):
            try:
                async with session.get(url) as response:
                    response.raise_for_status()
                    return await response.json()
            except Exception as e:
                last_exc = e
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)
    raise last_exc

async def fetch_api_data(api_url, cache_key):
    if cache_key in cache:
        return cache[cache_key]
    try:
        data = await async_get(api_url)
        cache[cache_key] = data
        return data
    except Exception as fetch_err:
        logger.error(f"Error al obtener datos: {str(fetch_err)}")
        raise

async def check_link_status(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                return "✅" if response.status == 200 else "❌"
    except aiohttp.ClientError:
        return "❌"

async def send_long_message(update, context, text, reply_markup=None, parse_mode="HTML"):
    max_length = 4096
    if len(text) <= max_length:
        message = await (update.message.reply_text if update.message else update.callback_query.message.reply_text)(
            text, reply_markup=reply_markup, parse_mode=parse_mode
        )
        return [message]
    parts = []
    current_part = ""
    lines = text.split("\n")
    for line in lines:
        if len(current_part) + len(line) + 1 > max_length:
            if current_part:
                parts.append(current_part.strip())
            current_part = line + "\n"
        else:
            current_part += line + "\n"
    if current_part:
        parts.append(current_part.strip())
    messages = []
    for i, part in enumerate(parts):
        message = await (update.message.reply_text if update.message else update.callback_query.message.reply_text)(
            part, reply_markup=reply_markup if i == len(parts) - 1 else None, parse_mode=parse_mode
        )
        messages.append(message)
    return messages

async def delete_messages(context, messages, exclude_protected=False, chat_id=None):
    if exclude_protected and chat_id:
        chat_data = context.bot_data.get('chat_data', {}).get(chat_id, {})
        start_messages = chat_data.get('start_messages', [])
        check_messages = chat_data.get('check_messages', [])
        protected_ids = {msg.message_id for msg in start_messages + check_messages}
        messages = [msg for msg in messages if msg.message_id not in protected_ids]
    for message in messages:
        try:
            await message.delete()
        except Exception as e:
            logger.warning(f"No se pudo eliminar mensaje: {str(e)}")

async def start(update, context):
    chat_id = update.message.chat_id
    ensure_chat_data(context, chat_id)
    keyboard = [
        [InlineKeyboardButton("ℹ️ Sobre mí", callback_data="cmd_info")],
        [InlineKeyboardButton("🔗 Analizar lista M3U (URL)", callback_data="cmd_check_url")],
        [InlineKeyboardButton("📁 Escanear listas desde TXT", callback_data="cmd_scan_txt")],
        [InlineKeyboardButton("⚖️ Comparar dos listas M3U", callback_data="cmd_compare")],
        [InlineKeyboardButton("🔀 Fusionar 2-4 listas M3U", callback_data="cmd_fusion")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    messages = await send_long_message(
        update, context,
        "🎉 <b>¡Hola! Bienvenido a IPTVJOX3</b> 🎉\n\n"
        "Soy tu asistente para explorar listas IPTV. ¿Qué quieres hacer hoy?\n\n"
        "🛠️ <b>Opciones disponibles</b>:\n"
        "➡️ /start - Muestra este menú\n"
        "➡️ /info - Conoce más sobre mí\n"
        "➡️ /check - Explora contenido o analiza una URL M3U\n"
        "➡️ /comparar &lt;URL1&gt; &lt;URL2&gt; - Compara dos listas M3U\n"
        "➡️ /fusionar - Fusiona 2 a 4 listas en una sola\n\n"
        "¡Vamos a explorar juntos! 🚀",
        reply_markup=reply_markup
    )
    context.bot_data['chat_data'][chat_id]['start_messages'] = messages
    context.bot_data['chat_data'][chat_id]['pending_messages'] = messages

async def info(update, context):
    chat_id = update.effective_chat.id
    ensure_chat_data(context, chat_id)
    await delete_messages(context, context.bot_data['chat_data'][chat_id].get('pending_messages', []), exclude_protected=True, chat_id=chat_id)
    context.bot_data['chat_data'][chat_id]['pending_messages'] = []
    response_text = (
        "🌟 <b>IPTVJOX3 - Creado por JOX_36</b> 🌟\n\n"
        "¡Hola! Soy un bot diseñado para ayudarte a gestionar y explorar listas IPTV de forma sencilla y divertida:\n\n"
        "📋 <b>¿Qué puedo hacer?</b>\n"
        "➡️ <code>/check &lt;URL&gt;</code> - Analizo tu lista M3U.\n"
        "➡️ <code>/start</code> - Muestra el menú principal.\n"
        "➡️ Envía un archivo <code>.txt</code> con URLs M3U para escaneo masivo.\n\n"
        "📌 <b>Tip</b>: Usa una URL M3U válida, como: <code>http://ejemplo.com/lista.m3u</code>.\n"
        f"⏳ <b>Última actualización</b>: {datetime.now().strftime('%Y-%m-%d')}\n\n"
        "¡Vamos a explorar juntos! 🚀"
    )
    messages = await send_long_message(update, context, response_text)
    context.bot_data['chat_data'][chat_id]['pending_messages'] = messages

async def parse_iptv_list_details(update, context, m3u_url, is_mass_scan=False):
    status = "No disponible"
    connections = "No disponible"
    created_at = "No disponible"
    expires_at = "No disponible"
    days_left = "No disponible"
    is_trial = "No"
    allowed_formats = "No disponible"
    channels_count = series_count = movies_count = 0
    epg_url = "No disponible"
    channel_categories_list = []
    series_categories_list = []
    movie_categories_list = []

    try:
        parsed_url = urllib.parse.urlparse(m3u_url)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        host = f"{parsed_url.scheme}://{parsed_url.netloc}"
        port = parsed_url.port or "No disponible"
        hostname = parsed_url.hostname or ""
        username = query_params.get("username", ["No disponible"])[0]
        password = query_params.get("password", ["No disponible"])[0]
        api_url = f"{host}/player_api.php?username={username}&password={password}"

        geo_task = asyncio.create_task(geolocate_host(hostname))

        try:
            data = await fetch_api_data(api_url, f"user_info_{host}_{username}")
            user_info = data.get("user_info", {})
            server_info = data.get("server_info", {})
            status = user_info.get("status", user_info.get("auth", "No disponible"))
            connections = f"{user_info.get('active_cons','0')}/{user_info.get('max_connections','0')}"
            if user_info.get("created_at"):
                try:
                    created_at = datetime.fromtimestamp(int(user_info["created_at"])).strftime("%Y-%m-%d %H:%M")
                except (ValueError, TypeError):
                    created_at = "Formato inválido"
            if user_info.get("exp_date"):
                try:
                    exp_date = datetime.fromtimestamp(int(user_info["exp_date"]))
                    expires_at = exp_date.strftime("%Y-%m-%d %H:%M")
                    days_left = (exp_date - datetime.now()).days
                except (ValueError, TypeError):
                    expires_at = "Formato inválido"
                    days_left = "N/A"
            tz_server = ""
            if server_info.get("server_display_name"):
                parts = server_info["server_display_name"].split("(")
                tz_server = parts[-1].replace(")", "").strip() if len(parts) > 1 else server_info.get("timezone", "")
            elif server_info.get("timezone"):
                tz_server = server_info["timezone"]
            is_trial = "Sí" if "trial" in str(status).lower() else "No"
            allowed_fmt = user_info.get("allowed_output_formats", server_info.get("allowed_output_formats", []))
            allowed_formats = ", ".join(allowed_fmt) if allowed_fmt else "No disponible"
            live_cats_task = asyncio.create_task(fetch_api_data(f"{api_url}&action=get_live_categories", f"live_categories_{host}_{username}"))
            series_cats_task = asyncio.create_task(fetch_api_data(f"{api_url}&action=get_series_categories", f"series_categories_{host}_{username}"))
            vod_cats_task = asyncio.create_task(fetch_api_data(f"{api_url}&action=get_vod_categories", f"vod_categories_{host}_{username}"))
            live_s_task = asyncio.create_task(fetch_api_data(f"{api_url}&action=get_live_streams", f"live_streams_{host}_{username}"))
            series_s_task = asyncio.create_task(fetch_api_data(f"{api_url}&action=get_series", f"series_streams_{host}_{username}"))
            vod_s_task = asyncio.create_task(fetch_api_data(f"{api_url}&action=get_vod_streams", f"vod_streams_{host}_{username}"))
            results = await asyncio.gather(live_cats_task, series_cats_task, vod_cats_task, live_s_task, series_s_task, vod_s_task, return_exceptions=True)
            live_cats_r, series_cats_r, vod_cats_r, live_s_r, series_s_r, vod_s_r = results
            channel_categories_list = [c["category_name"] for c in live_cats_r if isinstance(live_cats_r, list) and "category_name" in c]
            series_categories_list = [c["category_name"] for c in series_cats_r if isinstance(series_cats_r, list) and "category_name" in c]
            movie_categories_list = [c["category_name"] for c in vod_cats_r if isinstance(vod_cats_r, list) and "category_name" in c]
            channels_count = len(live_s_r) if isinstance(live_s_r, list) else 0
            series_count = len(series_s_r) if isinstance(series_s_r, list) else 0
            movies_count = len(vod_s_r) if isinstance(vod_s_r, list) else 0
            if server_info.get("url"):
                epg_url = f"{server_info['url']}/xmltv.php?username={username}&password={password}"
                if not epg_url.startswith(("http://", "https://")):
                    epg_url = f"http://{epg_url}"
            elif query_params.get("epg", [None])[0]:
                epg_url = query_params["epg"][0]
                if not epg_url.startswith(("http://", "https://")):
                    epg_url = f"http://{epg_url}"
        except Exception as api_err:
            logger.warning(f"API falló ({api_err}), parseando M3U directo.")
            try:
                loop = asyncio.get_event_loop()
                m3u_r = await loop.run_in_executor(None, safe_get, m3u_url)
                temp_cats = set()
                for i, line in enumerate(m3u_r.text.splitlines()):
                    if line.startswith("#EXTINF"):
                        channels_count += 1
                        gm = re.search(r'group-title="([^"]*)"', line)
                        temp_cats.add(gm.group(1) if gm else "Sin categoría")
                    elif "tvg-url=\"" in line or "url-tvg=\"" in line:
                        key = "tvg-url=\"" if "tvg-url=\"" in line else "url-tvg=\""
                        s = line.find(key) + len(key)
                        e_pos = line.find("\"", s)
                        epg_url = line[s:e_pos] if e_pos > s else "No disponible"
                channel_categories_list = sorted(temp_cats)
            except Exception as m3u_err:
                logger.error(f"Error parseando M3U: {m3u_err}")

        tz_geo, ciudad, pais, cod_pais, isp, ip_servidor = await geo_task
        tz_final = tz_server or tz_geo
        tz_bandera = timezone_a_bandera(tz_final)
        bandera_sv = codigo_a_bandera(cod_pais)
        geo_str = (bandera_sv + " " + ", ".join(p for p in [ciudad, pais] if p)).strip() or "N/A"
        isp_str = isp or "N/A"
        tz_display = f"{tz_final} {tz_bandera}".strip() if tz_final else "No disponible"
        url_ip = resolver_ip_m3u(host, username, password)
        if not isinstance(days_left, int):
            semaforo = ""
        elif days_left < 10:
            semaforo = "🔴"
        elif days_left < 20:
            semaforo = "🟠"
        else:
            semaforo = "🟢"
        scan_time = datetime.now().strftime("%Y-%m-%d %H:%M")
        response_text = (
            f"🎉 <b>=== Resultado M3U por JOX3 ===</b> 🎉\n\n"
            f"🌐 <b>Host</b>: <code>{escape_html(host)}</code>\n"
            f"🔌 <b>Puerto</b>: {escape_html(str(port))}\n"
            f"👤 <b>Usuario</b>: <code>{escape_html(username)}</code>\n"
            f"🔒 <b>Contraseña</b>: <code>{escape_html(password)}</code>\n"
            f"{'✅' if str(status).lower() == 'active' else '❌'} <b>Estado</b>: {escape_html(str(status))}\n"
            f"🔗 <b>Conexiones</b>: {escape_html(connections)}\n"
            f"📅 <b>Creado</b>: {escape_html(created_at)}\n"
            f"⏰ <b>Vencimiento</b>: {escape_html(expires_at)}\n"
            f"⌛ <b>Días restantes</b>: {escape_html(str(days_left))} {semaforo}\n"
            f"🕒 <b>Timezone</b>: {escape_html(tz_display)}\n"
            f"📍 <b>Ubicación</b>: {escape_html(geo_str)}\n"
            f"🏢 <b>ISP</b>: {escape_html(isp_str)}\n"
            f"🧪 <b>Prueba</b>: {escape_html(is_trial)}\n"
            f"🎞️ <b>Formatos</b>: {escape_html(allowed_formats)}\n"
            f"📺 <b>Enlace M3U</b>: <code>{escape_html(m3u_url)}</code>\n"
            f"📝 <b>Enlace EPG</b>: <code>{escape_html(epg_url)}</code>\n"
        )
        if url_ip:
            response_text += f"📌 <b>M3U por IP</b>: <code>{escape_html(url_ip)}</code>\n"
        response_text += (
            f"\n📡 <b>TV en vivo</b> ({channels_count}): "
            f"{escape_html(', '.join(channel_categories_list)) if channel_categories_list else 'No disponible'}\n\n"
            f"🎬 <b>Series</b> ({series_count}): "
            f"{escape_html(', '.join(series_categories_list)) if series_categories_list else 'No disponible'}\n\n"
            f"🎥 <b>Películas</b> ({movies_count}): "
            f"{escape_html(', '.join(movie_categories_list)) if movie_categories_list else 'No disponible'}\n\n"
            f"📅 <b>Escaneado</b>: {scan_time}\n"
        )
        return response_text, "success"
    except requests.exceptions.RequestException as req_err:
        return f"⚠️ No pude conectar con el servidor: {escape_html(str(req_err))}", "error"
    except ValueError as val_err:
        return f"⚠️ Formato de lista no válido: {escape_html(str(val_err))}", "error"
    except KeyError as key_err:
        return f"⚠️ Datos incompletos en la respuesta: {escape_html(str(key_err))}", "error"
    except Exception as gen_err:
        logger.error(f"Error inesperado: {gen_err}", exc_info=True)
        return f"⚠️ Error inesperado: {escape_html(str(gen_err))}", "error"

async def check_list(update, context, m3u_url_arg=None):
    chat_id = update.effective_chat.id
    ensure_chat_data(context, chat_id)
    await delete_messages(context, context.bot_data['chat_data'][chat_id].get('pending_messages', []), exclude_protected=True, chat_id=chat_id)
    context.bot_data['chat_data'][chat_id]['pending_messages'] = []
    m3u_url = m3u_url_arg if m3u_url_arg else (context.args[0] if context.args else None)
    if not m3u_url:
        messages = await send_long_message(update, context, "❌ Necesito una URL. Ejemplo: <code>/check http://ejemplo.com/lista.m3u</code>.")
        context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
        return
    parsed_url = urllib.parse.urlparse(m3u_url)
    if not parsed_url.scheme or not parsed_url.netloc:
        messages = await send_long_message(update, context, "❌ URL inválida. Incluye <code>http://</code> o <code>https://</code>.")
        context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
        return
    context.bot_data['chat_data'][chat_id]['m3u_url'] = m3u_url
    query_params = urllib.parse.parse_qs(parsed_url.query)
    host = f"{parsed_url.scheme}://{parsed_url.netloc}"
    username = query_params.get('username', ['No disponible'])[0]
    password = query_params.get('password', ['No disponible'])[0]
    api_url = f"{host}/player_api.php?username={username}&password={password}"
    status_message = await (update.message.reply_text if update.message else update.callback_query.message.reply_text)("⏳ Analizando la lista M3U... Por favor, espera...")
    channel_categories = []
    series_categories = []
    movie_categories = []
    channels_count = 0
    series_count = 0
    movies_count = 0
    try:
        channel_categories = [{'name': cat['category_name'], 'id': str(cat['category_id'])} for cat in (await fetch_api_data(f"{api_url}&action=get_live_categories", f"live_categories_{host}_{username}"))]
        channels = await fetch_api_data(f"{api_url}&action=get_live_streams", f"live_streams_{host}_{username}")
        channels_count = len(channels)
        context.bot_data['chat_data'][chat_id]['preloaded_channels'] = channels
    except Exception as e:
        logger.warning(f"Error canales: {str(e)}")
        try:
            loop = asyncio.get_event_loop()
            m3u_response = await loop.run_in_executor(None, safe_get, m3u_url)
            lines = m3u_response.text.splitlines()
            categories = set()
            channels = []
            for i, line in enumerate(lines):
                if line.startswith('#EXTINF'):
                    channels_count += 1
                    group_match = re.search(r'group-title="([^"]*)"', line)
                    current_group = group_match.group(1) if group_match else "Sin categoría"
                    channel_name = line.split('",')[1].strip() if '",' in line else "Sin nombre"
                    link = lines[i+1].strip() if i+1 < len(lines) else "No disponible"
                    channels.append({'name': channel_name, 'category_id': current_group, 'link': link, 'category_name': current_group})
                    categories.add(current_group)
            channel_categories = [{'name': cat, 'id': cat} for cat in categories]
            context.bot_data['chat_data'][chat_id]['preloaded_channels'] = channels
        except Exception:
            pass
    try:
        series_categories = [{'name': cat['category_name'], 'id': str(cat['category_id'])} for cat in (await fetch_api_data(f"{api_url}&action=get_series_categories", f"series_categories_{host}_{username}"))]
        series = await fetch_api_data(f"{api_url}&action=get_series", f"series_streams_{host}_{username}")
        series_count = len(series)
        context.bot_data['chat_data'][chat_id]['preloaded_series'] = series
    except Exception as e:
        logger.warning(f"Error series: {str(e)}")
    try:
        movie_categories = [{'name': cat['category_name'], 'id': str(cat['category_id'])} for cat in (await fetch_api_data(f"{api_url}&action=get_vod_categories", f"vod_categories_{host}_{username}"))]
        movies = await fetch_api_data(f"{api_url}&action=get_vod_streams", f"vod_streams_{host}_{username}")
        movies_count = len(movies) if isinstance(movies, list) else 0
        context.bot_data['chat_data'][chat_id]['preloaded_movies'] = movies
    except Exception as e:
        logger.warning(f"Error peliculas: {str(e)}")
        movie_categories = []
        movies_count = 0
    context.bot_data['chat_data'][chat_id]['channel_categories'] = channel_categories
    context.bot_data['chat_data'][chat_id]['series_categories'] = series_categories
    context.bot_data['chat_data'][chat_id]['movie_categories'] = movie_categories
    context.bot_data['chat_data'][chat_id]['counts'] = {
        'channels': channels_count, 'series': series_count, 'movies': movies_count,
        'channel_cats': len(channel_categories), 'series_cats': len(series_categories), 'movie_cats': len(movie_categories)
    }
    await status_message.delete()
    response_text = (
        f"🎥 <b>Explorando tu lista M3U</b> 🎥\n\n"
        f"📺 <b>Canales</b>: {channels_count} en {len(channel_categories)} categorías\n"
        f"📽️ <b>Series</b>: {series_count} en {len(series_categories)} categorías\n"
        f"🎬 <b>Películas</b>: {movies_count} en {len(movie_categories)} categorías\n\n"
        f"🕒 <b>Escaneado</b>: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        f"👇 ¡Elige qué quieres explorar!"
    )
    keyboard = [
        [InlineKeyboardButton("📺 Canales", callback_data="action_channels"), InlineKeyboardButton("📽️ Series", callback_data="action_series")],
        [InlineKeyboardButton("🎬 Películas", callback_data="action_movies"), InlineKeyboardButton("🔎 Buscar", callback_data="action_search")],
        [InlineKeyboardButton("📋 Información detallada de la lista", callback_data="action_list_info")]
    ]
    messages = await send_long_message(update, context, response_text, reply_markup=InlineKeyboardMarkup(keyboard))
    context.bot_data['chat_data'][chat_id]['check_messages'] = messages
    context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
    context.bot_data['chat_data'][chat_id]['m3u_url_for_info'] = m3u_url

async def display_list_info_from_check(update, context):
    chat_id = update.effective_chat.id
    ensure_chat_data(context, chat_id)
    await delete_messages(context, context.bot_data['chat_data'][chat_id].get('pending_messages', []), exclude_protected=True, chat_id=chat_id)
    context.bot_data['chat_data'][chat_id]['pending_messages'] = []
    m3u_url = context.bot_data['chat_data'][chat_id].get('m3u_url_for_info')
    if not m3u_url:
        messages = await send_long_message(update, context, "❌ No se encontró URL previa. Usa /check primero.")
        context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
        return
    status_message = await (update.callback_query.message.reply_text if update.callback_query else update.message.reply_text)("⏳ Obteniendo información detallada...")
    response_text, status = await parse_iptv_list_details(update, context, m3u_url)
    await status_message.delete()
    messages = await send_long_message(update, context, response_text)
    context.bot_data['chat_data'][chat_id]['pending_messages'] = messages

async def handle_document(update, context):
    chat_id = update.message.chat_id
    ensure_chat_data(context, chat_id)
    if update.message.document and update.message.document.mime_type == 'text/plain':
        file_id = update.message.document.file_id
        new_file = await context.bot.get_file(file_id)
        status_message = await update.message.reply_text("⏳ Procesando archivo TXT...")
        try:
            file_stream = BytesIO()
            await new_file.download_to_memory(file_stream)
            file_stream.seek(0)
            content = file_stream.read().decode('utf-8')
            urls = [line.strip() for line in content.splitlines() if line.strip().startswith(('http://', 'https://'))]
            if not urls:
                await status_message.delete()
                await send_long_message(update, context, "❌ El archivo no contiene URLs válidas.")
                return
            results = []
            for i, url in enumerate(urls):
                await status_message.edit_text(f"⏳ Escaneando URL {i+1}/{len(urls)}...")
                result_text, result_status = await parse_iptv_list_details(update, context, url, is_mass_scan=True)
                clean_text = strip_html(result_text)
                results.append(f"--- Resultado para {url} ({result_status.upper()}) ---\n{clean_text}\n\n")
            output_buffer = BytesIO()
            output_buffer.write("\n".join(results).encode('utf-8'))
            output_buffer.seek(0)
            await status_message.delete()
            await update.message.reply_document(document=output_buffer, filename="mass_scan_results.txt", caption=f"✅ Escaneo completado. {len(urls)} URLs analizadas.")
        except Exception as e:
            logger.error(f"Error TXT: {str(e)}")
            await status_message.delete()
            await send_long_message(update, context, f"⚠️ Error: {escape_html(str(e))}")

async def search(update, context, query=None, term=None):
    chat_id = update.effective_chat.id if query else update.message.chat_id
    ensure_chat_data(context, chat_id)
    await delete_messages(context, context.bot_data['chat_data'][chat_id].get('pending_messages', []), exclude_protected=True, chat_id=chat_id)
    context.bot_data['chat_data'][chat_id]['pending_messages'] = []
    status_message = await (update.callback_query.message.reply_text if query else update.message.reply_text)("⏳ Buscando contenido...")
    try:
        m3u_url = context.bot_data['chat_data'].get(chat_id, {}).get('m3u_url')
        if not m3u_url:
            await status_message.delete()
            messages = await send_long_message(query or update, context, "❌ Primero usa /check con una URL válida.")
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
        if not term:
            await status_message.delete()
            messages = await send_long_message(query or update, context, "❌ Escribe un término de búsqueda.")
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
        parsed_url = urllib.parse.urlparse(m3u_url)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        username = query_params.get('username', ['No disponible'])[0]
        password = query_params.get('password', ['No disponible'])[0]
        host = f"{parsed_url.scheme}://{parsed_url.netloc}"
        api_url = f"{host}/player_api.php?username={username}&password={password}"
        context.bot_data['chat_data'][chat_id]['search_results'] = {'channels': [], 'series': [], 'movies': []}
        selected_category = context.bot_data['chat_data'][chat_id].get('selected_category')
        categories_dict = {'live': {}, 'series': {}, 'vod': {}}
        cat_tasks = {}
        if not selected_category or selected_category == 'channels':
            cat_tasks['live'] = asyncio.create_task(fetch_api_data(f"{api_url}&action=get_live_categories", f"live_categories_{host}_{username}"))
        if not selected_category or selected_category == 'series':
            cat_tasks['series'] = asyncio.create_task(fetch_api_data(f"{api_url}&action=get_series_categories", f"series_categories_{host}_{username}"))
        if not selected_category or selected_category == 'movies':
            cat_tasks['vod'] = asyncio.create_task(fetch_api_data(f"{api_url}&action=get_vod_categories", f"vod_categories_{host}_{username}"))
        for key, task in cat_tasks.items():
            try:
                raw = await task
                categories_dict[key] = {str(cat['category_id']): cat['category_name'] for cat in raw}
            except Exception as e:
                logger.warning(f"Error categorías [{key}]: {str(e)}")
        term_lower = term.lower()
        pending_checks = []
        if not selected_category or selected_category == 'channels':
            channels = context.bot_data['chat_data'][chat_id].get('preloaded_channels', [])
            if not channels:
                try:
                    channels = await fetch_api_data(f"{api_url}&action=get_live_streams", f"live_streams_{host}_{username}")
                except Exception:
                    channels = []
            for channel in channels:
                if term_lower in str(channel.get('name', '')).lower():
                    channel_name = channel.get('name', 'Sin nombre')
                    category_name = categories_dict['live'].get(str(channel.get('category_id')), channel.get('category_name', 'Sin categoría'))
                    stream_id = channel.get('stream_id', 'No disponible')
                    link = f"{host}/live/{username}/{password}/{stream_id}.ts" if stream_id != 'No disponible' else channel.get('link', None)
                    if link:
                        pending_checks.append(('channels', {'name': channel_name, 'category': category_name, 'link': link}, link))
        if not selected_category or selected_category == 'series':
            series = context.bot_data['chat_data'][chat_id].get('preloaded_series', [])
            if not series:
                try:
                    series = await fetch_api_data(f"{api_url}&action=get_series", f"series_streams_{host}_{username}")
                except Exception:
                    series = []
            for serie in series:
                if term_lower in str(serie.get('name', '')).lower():
                    serie_name = serie.get('name', 'Sin nombre')
                    category_name = categories_dict['series'].get(str(serie.get('category_id')), 'Sin categoría')
                    series_id = serie.get('series_id', 'No disponible')
                    link = f"{host}/series/{username}/{password}/{series_id}.mp4" if series_id != 'No disponible' else None
                    if link:
                        pending_checks.append(('series', {'name': serie_name, 'category': category_name, 'link': link}, link))
        if not selected_category or selected_category == 'movies':
            movies = context.bot_data['chat_data'][chat_id].get('preloaded_movies', [])
            if not movies:
                try:
                    movies = await fetch_api_data(f"{api_url}&action=get_vod_streams", f"vod_streams_{host}_{username}")
                except Exception:
                    movies = []
            for movie in movies:
                if term_lower in str(movie.get('name', '')).lower():
                    movie_name = movie.get('name', 'Sin nombre')
                    category_name = categories_dict['vod'].get(str(movie.get('category_id')), 'Sin categoría')
                    vod_id = movie.get('stream_id', 'No disponible')
                    link = f"{host}/movie/{username}/{password}/{vod_id}.mp4" if vod_id != 'No disponible' else None
                    if link:
                        pending_checks.append(('movies', {'name': movie_name, 'category': category_name, 'link': link}, link))
        if pending_checks:
            statuses = await asyncio.gather(*[check_link_status(lnk) for _, _, lnk in pending_checks])
            for (section, item_data, _), status in zip(pending_checks, statuses):
                item_data['status'] = status
                context.bot_data['chat_data'][chat_id]['search_results'][section].append(item_data)
        keyboard = []
        if context.bot_data['chat_data'][chat_id]['search_results']['channels']:
            keyboard.append([InlineKeyboardButton("📺 Canales", callback_data="search_channels")])
        if context.bot_data['chat_data'][chat_id]['search_results']['series']:
            keyboard.append([InlineKeyboardButton("📽️ Series", callback_data="search_series")])
        if context.bot_data['chat_data'][chat_id]['search_results']['movies']:
            keyboard.append([InlineKeyboardButton("🎬 Películas", callback_data="search_movies")])
        await status_message.delete()
        if not keyboard:
            all_names = []
            for key in ['preloaded_channels', 'preloaded_series', 'preloaded_movies']:
                items = context.bot_data['chat_data'][chat_id].get(key, [])
                all_names.extend(str(item.get('name', '')) for item in items)
            suggestions = difflib.get_close_matches(term, all_names, n=3, cutoff=0.6)
            suggestion_text = f"\n\n📌 ¿Quizás quisiste decir? {escape_html(', '.join(suggestions))}" if suggestions else ""
            messages = await send_long_message(query or update, context, f"😔 No encontré nada para '{escape_html(term)}'.{suggestion_text}")
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
        messages = await send_long_message(query or update, context, f"🔍 <b>Resultados para '{escape_html(term)}'</b>\n\nSelecciona una categoría:", reply_markup=InlineKeyboardMarkup(keyboard))
        context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
    except Exception as e:
        await status_message.delete()
        messages = await send_long_message(query or update, context, f"⚠️ Error: {escape_html(str(e))}")
        context.bot_data['chat_data'][chat_id]['pending_messages'] = messages

async def handle_text(update, context):
    chat_id = update.message.chat_id
    ensure_chat_data(context, chat_id)
    await delete_messages(context, context.bot_data['chat_data'][chat_id].get('pending_messages', []), exclude_protected=True, chat_id=chat_id)
    context.bot_data['chat_data'][chat_id]['pending_messages'] = []
    if context.bot_data['chat_data'][chat_id].get('awaiting_category', False):
        text = update.message.text.strip().lower()
        category_map = {'canales': 'channels', 'series': 'series', 'películas': 'movies', 'peliculas': 'movies'}
        selected_category = category_map.get(text)
        if selected_category:
            context.bot_data['chat_data'][chat_id]['selected_category'] = selected_category
            context.bot_data['chat_data'][chat_id]['awaiting_category'] = False
            context.bot_data['chat_data'][chat_id]['awaiting_search'] = True
            messages = await send_long_message(update, context, "🔎 Escribe el término de búsqueda:")
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
        else:
            messages = await send_long_message(update, context, "❌ Escribe 'canales', 'series' o 'películas'.")
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
        return
    if context.bot_data['chat_data'][chat_id].get('awaiting_search', False):
        term = update.message.text.strip()
        context.bot_data['chat_data'][chat_id]['awaiting_search'] = False
        await search(update, context, term=term)
    elif context.bot_data['chat_data'][chat_id].get('awaiting_add', False):
        response = update.message.text.strip().lower()
        context.bot_data['chat_data'][chat_id]['awaiting_add'] = False
        last_item = context.bot_data['chat_data'][chat_id].get('last_item', {})
        if response in ['sí', 'si', 'yes'] and last_item:
            context.bot_data['chat_data'][chat_id]['custom_list'].append(last_item)
            messages = await send_long_message(update, context, f"✅ ¡{escape_html(last_item['name'])} añadido!\n\n¿Buscar otro?",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Sí", callback_data="continue_search"), InlineKeyboardButton("❌ No", callback_data="generate_list")]]))
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
        else:
            messages = await send_long_message(update, context, "🔍 ¿Buscar otro?",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Sí", callback_data="continue_search"), InlineKeyboardButton("❌ No", callback_data="generate_list")]]))
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
    elif context.bot_data['chat_data'][chat_id].get('awaiting_check_url', False):
        m3u_url = update.message.text.strip()
        context.bot_data['chat_data'][chat_id]['awaiting_check_url'] = False
        await check_list(update, context, m3u_url_arg=m3u_url)
    elif context.bot_data['chat_data'][chat_id].get('awaiting_compare_url1', False):
        url1 = update.message.text.strip()
        context.bot_data['chat_data'][chat_id]['awaiting_compare_url1'] = False
        context.bot_data['chat_data'][chat_id]['compare_url1'] = url1
        context.bot_data['chat_data'][chat_id]['awaiting_compare_url2'] = True
        messages = await send_long_message(update, context, f"✅ <b>Lista A</b>: <code>{escape_html(url1)}</code>\n\nEnvía la <b>Lista B</b>:")
        context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
    elif context.bot_data['chat_data'][chat_id].get('awaiting_compare_url2', False):
        url2 = update.message.text.strip()
        url1 = context.bot_data['chat_data'][chat_id].get('compare_url1', '')
        context.bot_data['chat_data'][chat_id]['awaiting_compare_url2'] = False
        context.bot_data['chat_data'][chat_id]['compare_url1'] = None
        await comparar_listas(update, context, url1=url1, url2=url2)
    elif context.bot_data['chat_data'][chat_id].get('awaiting_fusion_url', False):
        texto = update.message.text.strip()
        urls = context.bot_data['chat_data'][chat_id].get('fusion_urls', [])
        p = urllib.parse.urlparse(texto)
        if not p.scheme or not p.netloc:
            messages = await send_long_message(update, context, "❌ URL inválida. Envía de nuevo:")
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
        urls.append(texto)
        context.bot_data['chat_data'][chat_id]['fusion_urls'] = urls
        n = len(urls)
        if n < 2:
            messages = await send_long_message(update, context, f"✅ Lista {n} recibida.\nEnvía la URL de la lista {n+1}:")
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
        elif n < 4:
            lista_txt = "\n".join(f"  {i+1}. <code>{escape_html(u)}</code>" for i, u in enumerate(urls))
            messages = await send_long_message(update, context, f"✅ Lista {n} recibida:\n{lista_txt}\n\n¿Agregar más o continuar?",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"➕ Lista {n+1}", callback_data="fusion_add_more"), InlineKeyboardButton("✅ Continuar", callback_data="fusion_done_urls")]]))
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            context.bot_data['chat_data'][chat_id]['awaiting_fusion_url'] = False
        else:
            context.bot_data['chat_data'][chat_id]['awaiting_fusion_url'] = False
            await _fusion_pedir_tipos(update, context)

async def display_search_results(update, context, section):
    chat_id = update.effective_chat.id
    ensure_chat_data(context, chat_id)
    await delete_messages(context, context.bot_data['chat_data'][chat_id].get('pending_messages', []), exclude_protected=True, chat_id=chat_id)
    context.bot_data['chat_data'][chat_id]['pending_messages'] = []
    try:
        query = update.callback_query
        await query.answer()
        results = context.bot_data['chat_data'].get(chat_id, {}).get('search_results', {}).get(section, [])
        if not results:
            messages = await send_long_message(update, context, f"😔 No hay resultados para {escape_html(section)}.")
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
        emoji = "📺" if section == "channels" else "📽️" if section == "series" else "🎬"
        keyboard = [[InlineKeyboardButton(result['name'], callback_data=f"result_{section}_{i}")] for i, result in enumerate(results)]
        messages = await send_long_message(update, context, f"{emoji} <b>{escape_html(section.capitalize())}</b>\n\nTotal: {len(results)}\n\n👇 Selecciona:", reply_markup=InlineKeyboardMarkup(keyboard))
        context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
    except Exception as e:
        messages = await send_long_message(update, context, f"⚠️ Error: {escape_html(str(e))}")
        context.bot_data['chat_data'][chat_id]['pending_messages'] = messages

async def list_paginated_categories(update, context, categories, counts, section, page=1):
    chat_id = update.effective_chat.id
    ensure_chat_data(context, chat_id)
    await delete_messages(context, context.bot_data['chat_data'][chat_id].get('pending_messages', []), exclude_protected=True, chat_id=chat_id)
    context.bot_data['chat_data'][chat_id]['pending_messages'] = []
    try:
        m3u_url = context.bot_data['chat_data'].get(chat_id, {}).get('m3u_url')
        if not m3u_url:
            messages = await send_long_message(update, context, "❌ Primero usa /check.")
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
        if not categories:
            messages = await send_long_message(update, context, f"😔 No hay categorías de {escape_html(section)}.")
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
        items_per_page = 50
        total_pages = math.ceil(len(categories) / items_per_page)
        page = max(1, min(page, total_pages))
        start_idx = (page - 1) * items_per_page
        page_categories = categories[start_idx:start_idx + items_per_page]
        emoji = "📺" if section == "channels" else "📽️" if section == "series" else "🎬"
        response_text = f"{emoji} <b>{escape_html(section.capitalize())}</b>\n\nTotal: {counts.get(section, 0)} · Página {page}/{total_pages}\n\n👇 Selecciona:"
        keyboard = [[InlineKeyboardButton(cat['name'], callback_data=f"{section}_{urllib.parse.quote_plus(str(cat['id']))}")] for cat in page_categories]
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("⬅️ Anterior", callback_data=f"page_{section}_{page-1}"))
        if page < total_pages:
            nav_buttons.append(InlineKeyboardButton("Siguiente ➡️", callback_data=f"page_{section}_{page+1}"))
        if nav_buttons:
            keyboard.append(nav_buttons)
        messages = await send_long_message(update, context, response_text, reply_markup=InlineKeyboardMarkup(keyboard))
        context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
    except Exception as e:
        messages = await send_long_message(update, context, f"⚠️ Error: {escape_html(str(e))}")
        context.bot_data['chat_data'][chat_id]['pending_messages'] = messages

async def list_channels(update, context, query=None, page=1):
    chat_id = update.effective_chat.id
    ensure_chat_data(context, chat_id)
    await list_paginated_categories(update, context, context.bot_data['chat_data'].get(chat_id, {}).get('channel_categories', []), context.bot_data['chat_data'].get(chat_id, {}).get('counts', {}), "channels", page)

async def list_series(update, context, query=None, page=1):
    chat_id = update.effective_chat.id
    ensure_chat_data(context, chat_id)
    await list_paginated_categories(update, context, context.bot_data['chat_data'].get(chat_id, {}).get('series_categories', []), context.bot_data['chat_data'].get(chat_id, {}).get('counts', {}), "series", page)

async def list_movies(update, context, query=None, page=1):
    chat_id = update.effective_chat.id
    ensure_chat_data(context, chat_id)
    await list_paginated_categories(update, context, context.bot_data['chat_data'].get(chat_id, {}).get('movie_categories', []), context.bot_data['chat_data'].get(chat_id, {}).get('counts', {}), "movies", page)

async def generate_custom_list(update, context):
    chat_id = update.effective_chat.id
    ensure_chat_data(context, chat_id)
    await delete_messages(context, context.bot_data['chat_data'][chat_id].get('pending_messages', []), exclude_protected=True, chat_id=chat_id)
    context.bot_data['chat_data'][chat_id]['pending_messages'] = []
    custom_list = context.bot_data['chat_data'].get(chat_id, {}).get('custom_list', [])
    if not custom_list:
        messages = await send_long_message(update, context, "😔 Tu lista personalizada está vacía.")
        context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
        return
    response_text = "📋 <b>Tu lista personalizada</b>\n\n"
    channels = [item for item in custom_list if item.get('type') == 'channel']
    series = [item for item in custom_list if item.get('type') == 'series']
    movies = [item for item in custom_list if item.get('type') == 'movie']
    if channels:
        response_text += "📺 <b>Canales</b>:\n" + "\n".join(f"- {escape_html(i['name'])} ({escape_html(i['category'])}) - {escape_html(i['status'])}" for i in channels) + "\n"
    if series:
        response_text += "\n📽️ <b>Series</b>:\n" + "\n".join(f"- {escape_html(i['name'])} ({escape_html(i['category'])}) - {escape_html(i['status'])}" for i in series) + "\n"
    if movies:
        response_text += "\n🎬 <b>Películas</b>:\n" + "\n".join(f"- {escape_html(i['name'])} ({escape_html(i['category'])}) - {escape_html(i['status'])}" for i in movies) + "\n"
    response_text += f"\n🎉 Total: {len(custom_list)} · {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    file_content = "#EXTM3U\n# Lista Personalizada IPTVJOX3\n"
    for item in custom_list:
        file_content += f'#EXTINF:-1 group-title="{item["category"]}",{item["name"]}\n{item["link"]}\n'
    buffer = BytesIO()
    buffer.write(file_content.encode('utf-8'))
    buffer.seek(0)
    messages = await send_long_message(update, context, response_text)
    context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
    await update.callback_query.message.reply_document(document=buffer, filename="custom_list.m3u", caption="📄 Tu lista personalizada.")
    context.bot_data['chat_data'][chat_id]['custom_list'] = []

async def handle_category_selection(update, context):
    chat_id = update.effective_chat.id
    ensure_chat_data(context, chat_id)
    await delete_messages(context, context.bot_data['chat_data'][chat_id].get('pending_messages', []), exclude_protected=True, chat_id=chat_id)
    context.bot_data['chat_data'][chat_id]['pending_messages'] = []
    try:
        query = update.callback_query
        await query.answer()
        callback_data = query.data
        if callback_data == "cmd_check_url":
            context.bot_data['chat_data'][chat_id]['awaiting_check_url'] = True
            messages = await send_long_message(update, context, "🔗 Envía la URL de tu lista M3U:")
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
        if callback_data == "cmd_scan_txt":
            messages = await send_long_message(update, context, "📁 Envíame un archivo <code>.txt</code> con URLs M3U.")
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
        if callback_data == "cmd_compare":
            context.bot_data['chat_data'][chat_id]['awaiting_compare_url1'] = True
            context.bot_data['chat_data'][chat_id]['compare_url1'] = None
            messages = await send_long_message(update, context, "⚖️ <b>Comparador</b>\n\nEnvía la <b>Lista A</b>:")
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
        if callback_data == "cmd_fusion":
            context.bot_data['chat_data'][chat_id]['fusion_urls'] = []
            context.bot_data['chat_data'][chat_id]['fusion_tipos'] = []
            context.bot_data['chat_data'][chat_id]['awaiting_fusion_url'] = True
            messages = await send_long_message(update, context, "🔀 <b>Fusionador</b>\n\nEnvía la <b>Lista 1</b>:")
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
        if callback_data == "fusion_add_more":
            context.bot_data['chat_data'][chat_id]['awaiting_fusion_url'] = True
            n = len(context.bot_data['chat_data'][chat_id].get('fusion_urls', []))
            messages = await send_long_message(update, context, f"Envía la <b>Lista {n+1}</b>:")
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
        if callback_data == "fusion_done_urls":
            await _fusion_pedir_tipos(update, context)
            return
        if callback_data.startswith("fusion_tipo_"):
            tipo = callback_data.replace("fusion_tipo_", "")
            tipos = context.bot_data['chat_data'][chat_id].get('fusion_tipos', [])
            if tipo in tipos:
                tipos.remove(tipo)
            else:
                tipos.append(tipo)
            context.bot_data['chat_data'][chat_id]['fusion_tipos'] = tipos
            await _fusion_pedir_tipos(update, context, editar=True)
            return
        if callback_data == "fusion_confirmar":
            tipos = context.bot_data['chat_data'][chat_id].get('fusion_tipos', [])
            if not tipos:
                messages = await send_long_message(update, context, "❌ Selecciona al menos un tipo.")
                context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
                return
            await fusionar_listas(update, context)
            return
        if callback_data == "continue_search":
            context.bot_data['chat_data'][chat_id]['selected_category'] = None
            messages = await send_long_message(update, context, "🔎 ¿Dónde buscar?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📺 Canales", callback_data="select_category_channels"), InlineKeyboardButton("📽️ Series", callback_data="select_category_series")],
                    [InlineKeyboardButton("🎬 Películas", callback_data="select_category_movies")]
                ]))
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
        if callback_data.startswith("select_category_"):
            category = callback_data.replace("select_category_", "")
            context.bot_data['chat_data'][chat_id]['selected_category'] = category
            context.bot_data['chat_data'][chat_id]['awaiting_search'] = True
            messages = await send_long_message(update, context, f"🔎 Buscando en <b>{escape_html(category)}</b>. Escribe el término:")
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
        if callback_data == "generate_list":
            await generate_custom_list(update, context)
            return
        if callback_data.startswith("cmd_"):
            command = callback_data.replace("cmd_", "")
            if command == "info":
                await info(update, context)
            return
        if callback_data.startswith("action_"):
            action = callback_data.replace("action_", "")
            if action == "channels":
                await list_channels(update, context, query)
            elif action == "series":
                await list_series(update, context, query)
            elif action == "movies":
                await list_movies(update, context, query)
            elif action == "search":
                context.bot_data['chat_data'][chat_id]['selected_category'] = None
                context.bot_data['chat_data'][chat_id]['awaiting_search'] = True
                messages = await send_long_message(update, context, "🔎 Escribe qué quieres buscar:")
                context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            elif action == "list_info":
                await display_list_info_from_check(update, context)
            return
        if callback_data.startswith("search_"):
            await display_search_results(update, context, callback_data.replace("search_", ""))
            return
        if callback_data.startswith("result_"):
            parts = callback_data.split("_")
            if len(parts) < 3:
                return
            section = parts[1]
            index = int(parts[2])
            results = context.bot_data['chat_data'].get(chat_id, {}).get('search_results', {}).get(section, [])
            if index < 0 or index >= len(results):
                return
            result = results[index]
            result['type'] = section[:-1]
            context.bot_data['chat_data'][chat_id]['last_item'] = result
            emoji = "📺" if section == "channels" else "📽️" if section == "series" else "🎬"
            response_text = (f"{emoji} <b>{escape_html(result['name'])}</b>\n\n"
                f"📂 {escape_html(result['category'])}\n"
                f"🔗 <code>{escape_html(result['link'])}</code> {escape_html(result['status'])}\n\n➕ ¿Añadir a tu lista?")
            messages = await send_long_message(update, context, response_text,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Sí", callback_data=f"add_yes_{section}_{index}"), InlineKeyboardButton("❌ No", callback_data=f"add_no_{section}_{index}")]]))
            context.bot_data['chat_data'][chat_id]['awaiting_add'] = True
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
        if callback_data.startswith("add_yes_"):
            parts = callback_data.split("_")
            if len(parts) < 4:
                return
            section = parts[2]
            index = int(parts[3])
            results = context.bot_data['chat_data'].get(chat_id, {}).get('search_results', {}).get(section, [])
            last_item = context.bot_data['chat_data'].get(chat_id, {}).get('last_item', {})
            item = results[index] if index < len(results) else last_item
            item['type'] = section[:-1]
            context.bot_data['chat_data'][chat_id]['custom_list'].append(item)
            context.bot_data['chat_data'][chat_id]['awaiting_add'] = False
            messages = await send_long_message(update, context, f"✅ ¡{escape_html(item['name'])} añadido!\n\n¿Buscar otro?",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Sí", callback_data="continue_search"), InlineKeyboardButton("❌ No", callback_data="generate_list")]]))
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
        if callback_data.startswith("add_no_"):
            context.bot_data['chat_data'][chat_id]['awaiting_add'] = False
            messages = await send_long_message(update, context, "🔍 ¿Buscar otro?",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Sí", callback_data="continue_search"), InlineKeyboardButton("❌ No", callback_data="generate_list")]]))
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
        if callback_data.startswith("page_"):
            parts = callback_data.split("_")
            if len(parts) != 3:
                return
            section, page = parts[1], int(parts[2])
            if section == "channels":
                await list_channels(update, context, query, page)
            elif section == "series":
                await list_series(update, context, query, page)
            elif section == "movies":
                await list_movies(update, context, query, page)
            return
        data_parts = callback_data.split('_', 1)
        if len(data_parts) != 2:
            return
        action, value = data_parts
        m3u_url = context.bot_data['chat_data'].get(chat_id, {}).get('m3u_url')
        if not m3u_url:
            messages = await send_long_message(update, context, "❌ Usa /check primero.")
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
        parsed_url = urllib.parse.urlparse(m3u_url)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        username = query_params.get('username', ['No disponible'])[0]
        password = query_params.get('password', ['No disponible'])[0]
        host = f"{parsed_url.scheme}://{parsed_url.netloc}"
        api_url = f"{host}/player_api.php?username={username}&password={password}"
        if action in ["channels", "series", "movies"]:
            categories_key = f"{action}_categories"
            categories = context.bot_data['chat_data'].get(chat_id, {}).get(categories_key, [])
            category = next((cat for cat in categories if str(cat['id']) == str(urllib.parse.unquote_plus(value))), None)
            if not category:
                try:
                    api_action_cat = 'live' if action == 'channels' else 'series' if action == 'series' else 'vod'
                    new_categories = [{'name': cat['category_name'], 'id': str(cat['category_id'])} for cat in (await fetch_api_data(f"{api_url}&action=get_{api_action_cat}_categories", f"{action}_categories_{host}_{username}"))]
                    context.bot_data['chat_data'][chat_id][categories_key] = new_categories
                    category = next((cat for cat in new_categories if str(cat['id']) == str(urllib.parse.unquote_plus(value))), None)
                except Exception:
                    pass
            if not category:
                messages = await send_long_message(update, context, "😔 No encontré la categoría. Intenta /check de nuevo.")
                context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
                return
            api_action = "get_live_streams" if action == "channels" else "get_series" if action == "series" else "get_vod_streams"
            try:
                streams = await fetch_api_data(f"{api_url}&action={api_action}&category_id={category['id']}", f"{api_action}_{category['id']}_{host}_{username}")
                if not streams:
                    messages = await send_long_message(update, context, f"😔 No hay contenido en <b>{escape_html(category['name'])}</b>.")
                    context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
                    return
                emoji = '📺' if action == 'channels' else '📽️' if action == 'series' else '🎬'
                link_type = 'channel' if action == 'channels' else 'series' if action == 'series' else 'movie'
                keyboard = [[InlineKeyboardButton(stream.get('name', 'Sin nombre'), callback_data=f"link_{link_type}_{i}")] for i, stream in enumerate(streams)]
                context.bot_data['chat_data'][chat_id][action] = streams
                messages = await send_long_message(update, context, f"{emoji} <b>{escape_html(category['name'])}</b>\n\nTotal: {len(streams)}\n\n👇 Selecciona:", reply_markup=InlineKeyboardMarkup(keyboard))
                context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            except Exception as e:
                messages = await send_long_message(update, context, f"⚠️ Error: {escape_html(str(e))}")
                context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
        elif action == "link":
            content_type, index = value.split('_', 1)
            index = int(index)
            content_key = 'channels' if content_type == 'channel' else 'series' if content_type == 'series' else 'movies'
            items = context.bot_data['chat_data'].get(chat_id, {}).get(content_key, [])
            if index < 0 or index >= len(items):
                return
            item = items[index]
            item_name = item.get('name', 'Sin nombre')
            if content_type == 'series':
                stream_id = item.get('series_id')
                if not stream_id:
                    await send_long_message(update, context, "😔 No pude obtener el enlace.")
                    return
                link = f"{host}/series/{username}/{password}/{stream_id}.mp4"
            elif content_type == 'channel':
                stream_id = item.get('stream_id')
                link = item.get('link') if not stream_id else f"{host}/live/{username}/{password}/{stream_id}.ts"
                if not link:
                    await send_long_message(update, context, "😔 No pude obtener el enlace.")
                    return
            else:
                stream_id = item.get('stream_id')
                if not stream_id:
                    await send_long_message(update, context, "😔 No pude obtener el enlace.")
                    return
                link = f"{host}/movie/{username}/{password}/{stream_id}.mp4"
            status = await check_link_status(link)
            item_data = {'name': item_name, 'category': item.get('category_name', 'Sin categoría'), 'link': link, 'status': status, 'type': content_type}
            context.bot_data['chat_data'][chat_id]['last_item'] = item_data
            emoji = '📺' if content_type == 'channel' else '📽️' if content_type == 'series' else '🎬'
            response_text = (f"{emoji} <b>{escape_html(item_name)}</b>\n\n"
                f"📂 {escape_html(item.get('category_name', 'Sin categoría'))}\n"
                f"🔗 <code>{escape_html(link)}</code> {escape_html(status)}\n\n➕ ¿Añadir a tu lista?")
            messages = await send_long_message(update, context, response_text,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Sí", callback_data=f"add_yes_{content_key}_{index}"), InlineKeyboardButton("❌ No", callback_data=f"add_no_{content_key}_{index}")]]))
            context.bot_data['chat_data'][chat_id]['awaiting_add'] = True
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
    except Exception as e:
        logger.error(f"Error en handle_category_selection: {str(e)}")
        messages = await send_long_message(update, context, f"⚠️ Error inesperado: {escape_html(str(e))}")
        context.bot_data['chat_data'][chat_id]['pending_messages'] = messages

async def _get_nombres_lista(m3u_url, host, username, password, api_url):
    resultado = {'channels': set(), 'series': set(), 'movies': set()}
    try:
        live_r, series_r, vod_r = await asyncio.gather(
            asyncio.create_task(fetch_api_data(f"{api_url}&action=get_live_streams", f"live_streams_{host}_{username}")),
            asyncio.create_task(fetch_api_data(f"{api_url}&action=get_series", f"series_streams_{host}_{username}")),
            asyncio.create_task(fetch_api_data(f"{api_url}&action=get_vod_streams", f"vod_streams_{host}_{username}")),
            return_exceptions=True
        )
        if isinstance(live_r, list):
            resultado['channels'] = {str(s.get('name', '')).strip().lower() for s in live_r if s.get('name')}
        if isinstance(series_r, list):
            resultado['series'] = {str(s.get('name', '')).strip().lower() for s in series_r if s.get('name')}
        if isinstance(vod_r, list):
            resultado['movies'] = {str(s.get('name', '')).strip().lower() for s in vod_r if s.get('name')}
        if resultado['channels'] or resultado['series'] or resultado['movies']:
            return resultado
    except Exception:
        pass
    try:
        loop = asyncio.get_event_loop()
        resp = await loop.run_in_executor(None, safe_get, m3u_url)
        for line in resp.text.splitlines():
            if line.startswith('#EXTINF'):
                name = line.split('",')[-1].strip() if '",' in line else line.split(',')[-1].strip()
                if name:
                    resultado['channels'].add(name.strip().lower())
    except Exception:
        pass
    return resultado

async def comparar_listas(update, context, url1=None, url2=None):
    chat_id = update.effective_chat.id
    ensure_chat_data(context, chat_id)
    for url, label in [(url1, "Lista A"), (url2, "Lista B")]:
        p = urllib.parse.urlparse(url)
        if not p.scheme or not p.netloc:
            messages = await send_long_message(update, context, f"❌ URL inválida para <b>{label}</b>.")
            context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
            return
    status_msg = await (update.message.reply_text if update.message else update.callback_query.message.reply_text)("⚖️ Comparando listas... Por favor espera...")
    try:
        def _parse(url):
            p = urllib.parse.urlparse(url)
            qp = urllib.parse.parse_qs(p.query)
            host = f"{p.scheme}://{p.netloc}"
            user = qp.get('username', [''])[0]
            pwd = qp.get('password', [''])[0]
            return host, user, pwd, f"{host}/player_api.php?username={user}&password={pwd}"
        host1, user1, pwd1, api1 = _parse(url1)
        host2, user2, pwd2, api2 = _parse(url2)
        datos1, datos2 = await asyncio.gather(
            _get_nombres_lista(url1, host1, user1, pwd1, api1),
            _get_nombres_lista(url2, host2, user2, pwd2, api2),
        )
        await status_msg.delete()
        tipos = [('channels', '📺 Canales'), ('series', '📽️ Series'), ('movies', '🎬 Películas')]
        resumen_lines = [f"⚖️ <b>Comparación de listas</b>\n", f"🅰️ <code>{escape_html(url1)}</code>", f"🅱️ <code>{escape_html(url2)}</code>\n"]
        detalle_lines = ["=== COMPARACIÓN DE LISTAS M3U por IPTVJOX3 ===", f"Lista A: {url1}", f"Lista B: {url2}", f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M')}", ""]
        total_solo_a = total_solo_b = total_comun = 0
        for key, emoji_label in tipos:
            set_a = datos1[key]
            set_b = datos2[key]
            solo_a = sorted(set_a - set_b)
            solo_b = sorted(set_b - set_a)
            comunes = sorted(set_a & set_b)
            total_solo_a += len(solo_a)
            total_solo_b += len(solo_b)
            total_comun += len(comunes)
            pct_a = round(len(comunes) / len(set_a) * 100) if set_a else 0
            pct_b = round(len(comunes) / len(set_b) * 100) if set_b else 0
            resumen_lines += [f"\n{emoji_label}", f"  🅰️ {len(set_a):,}  |  🅱️ {len(set_b):,}", f"  🟰 Comunes: {len(comunes):,} ({pct_a}% A · {pct_b}% B)", f"  ➕ Solo A: {len(solo_a):,}  |  Solo B: {len(solo_b):,}"]
            detalle_lines += [f"{'='*50}", emoji_label, f"  Solo A ({len(solo_a)}):"] + [f"    - {n}" for n in solo_a] or ["    (ninguno)"]
            detalle_lines += [f"  Solo B ({len(solo_b)}):"] + [f"    - {n}" for n in solo_b] or ["    (ninguno)"]
            detalle_lines += [f"  Comunes ({len(comunes)}):"] + [f"    = {n}" for n in comunes] or ["    (ninguno)"]
            detalle_lines.append("")
        total_a = sum(len(datos1[k]) for k in datos1)
        total_b = sum(len(datos2[k]) for k in datos2)
        resumen_lines += [f"\n📊 <b>Global</b>  🅰️ {total_a:,}  |  🅱️ {total_b:,}", f"  🟰 Comunes: {total_comun:,}  |  Excl. A: {total_solo_a:,}  |  Excl. B: {total_solo_b:,}", f"\n📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}"]
        messages = await send_long_message(update, context, "\n".join(resumen_lines))
        context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
        buf = BytesIO()
        buf.write("\n".join(detalle_lines).encode('utf-8'))
        buf.seek(0)
        await (update.message.reply_document if update.message else update.callback_query.message.reply_document)(document=buf, filename="comparacion_listas.txt", caption="📄 Detalle completo.")
    except Exception as e:
        logger.error(f"Error comparar_listas: {e}", exc_info=True)
        try:
            await status_msg.delete()
        except Exception:
            pass
        messages = await send_long_message(update, context, f"⚠️ Error: {escape_html(str(e))}")
        context.bot_data['chat_data'][chat_id]['pending_messages'] = messages

async def cmd_comparar(update, context):
    chat_id = update.effective_chat.id
    ensure_chat_data(context, chat_id)
    args = context.args
    if len(args) == 2:
        await comparar_listas(update, context, url1=args[0], url2=args[1])
    elif len(args) == 1:
        context.bot_data['chat_data'][chat_id]['compare_url1'] = args[0]
        context.bot_data['chat_data'][chat_id]['awaiting_compare_url2'] = True
        messages = await send_long_message(update, context, f"✅ Lista A: <code>{escape_html(args[0])}</code>\n\nEnvía la Lista B:")
        context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
    else:
        messages = await send_long_message(update, context, "⚖️ Uso: <code>/comparar &lt;URL_A&gt; &lt;URL_B&gt;</code>")
        context.bot_data['chat_data'][chat_id]['pending_messages'] = messages

_QUALITY_KEYWORDS = [('4k', 100), ('4K', 100), ('uhd', 90), ('UHD', 90), ('fhd', 80), ('FHD', 80), ('1080', 75), ('hd', 60), ('HD', 60), ('720', 55), ('sd', 20), ('SD', 20)]

def _score_calidad(nombre):
    n = nombre.upper()
    for kw, score in _QUALITY_KEYWORDS:
        if kw.upper() in n:
            return score
    return 40

async def _medir_velocidad(session, url):
    try:
        import time
        t0 = time.monotonic()
        async with session.head(url, timeout=aiohttp.ClientTimeout(total=5), allow_redirects=True) as r:
            if r.status < 400:
                return (time.monotonic() - t0) * 1000
    except Exception:
        pass
    return 9999.0

async def _get_streams_completos(m3u_url, host, username, password, api_url, tipos):
    resultado = {'channels': [], 'series': [], 'movies': []}
    tasks = {}
    if 'channels' in tipos:
        tasks['channels'] = asyncio.create_task(fetch_api_data(f"{api_url}&action=get_live_streams", f"live_streams_{host}_{username}"))
        tasks['live_cats'] = asyncio.create_task(fetch_api_data(f"{api_url}&action=get_live_categories", f"live_cats_{host}_{username}"))
    if 'series' in tipos:
        tasks['series'] = asyncio.create_task(fetch_api_data(f"{api_url}&action=get_series", f"series_streams_{host}_{username}"))
        tasks['series_cats'] = asyncio.create_task(fetch_api_data(f"{api_url}&action=get_series_categories", f"series_cats_{host}_{username}"))
    if 'movies' in tipos:
        tasks['movies'] = asyncio.create_task(fetch_api_data(f"{api_url}&action=get_vod_streams", f"vod_streams_{host}_{username}"))
        tasks['vod_cats'] = asyncio.create_task(fetch_api_data(f"{api_url}&action=get_vod_categories", f"vod_cats_{host}_{username}"))
    fetched = {}
    for key, task in tasks.items():
        try:
            fetched[key] = await task
        except Exception:
            fetched[key] = []
    def build_cat_map(raw):
        return {str(c.get('category_id', '')): c.get('category_name', 'Sin categoría') for c in raw} if isinstance(raw, list) else {}
    if 'channels' in tipos:
        cat_map = build_cat_map(fetched.get('live_cats', []))
        for s in fetched.get('channels', []):
            if isinstance(fetched.get('channels'), list) and s.get('stream_id'):
                resultado['channels'].append({'name': str(s.get('name', 'Sin nombre')).strip(), 'link': f"{host}/live/{username}/{password}/{s['stream_id']}.ts", 'category': cat_map.get(str(s.get('category_id', '')), 'Sin categoría')})
    if 'series' in tipos:
        cat_map = build_cat_map(fetched.get('series_cats', []))
        for s in fetched.get('series', []):
            if isinstance(fetched.get('series'), list) and s.get('series_id'):
                resultado['series'].append({'name': str(s.get('name', 'Sin nombre')).strip(), 'link': f"{host}/series/{username}/{password}/{s['series_id']}.mp4", 'category': cat_map.get(str(s.get('category_id', '')), 'Sin categoría')})
    if 'movies' in tipos:
        cat_map = build_cat_map(fetched.get('vod_cats', []))
        for s in fetched.get('movies', []):
            if isinstance(fetched.get('movies'), list) and s.get('stream_id'):
                resultado['movies'].append({'name': str(s.get('name', 'Sin nombre')).strip(), 'link': f"{host}/movie/{username}/{password}/{s['stream_id']}.mp4", 'category': cat_map.get(str(s.get('category_id', '')), 'Sin categoría')})
    if 'channels' in tipos and not resultado['channels']:
        try:
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(None, safe_get, m3u_url)
            lines_all = resp.text.splitlines()
            for i, line in enumerate(lines_all):
                if line.startswith('#EXTINF'):
                    name = line.split('",')[-1].strip() if '",' in line else line.split(',')[-1].strip()
                    gm = re.search(r'group-title="([^"]*)"', line)
                    cat = gm.group(1) if gm else 'Sin categoría'
                    link = lines_all[i+1].strip() if i+1 < len(lines_all) else ''
                    if link and name:
                        resultado['channels'].append({'name': name, 'link': link, 'category': cat})
        except Exception:
            pass
    return resultado

async def _fusion_pedir_tipos(update, context, editar=False):
    chat_id = update.effective_chat.id
    tipos = context.bot_data['chat_data'][chat_id].get('fusion_tipos', [])
    urls = context.bot_data['chat_data'][chat_id].get('fusion_urls', [])
    def marca(t):
        return "✅" if t in tipos else "☑️"
    keyboard = [
        [InlineKeyboardButton(f"{marca('channels')} 📺 Live TV", callback_data="fusion_tipo_channels"), InlineKeyboardButton(f"{marca('series')} 📽️ Series", callback_data="fusion_tipo_series")],
        [InlineKeyboardButton(f"{marca('movies')} 🎬 VOD/Películas", callback_data="fusion_tipo_movies")],
        [InlineKeyboardButton("🔀 Fusionar ahora", callback_data="fusion_confirmar")],
    ]
    lista_txt = "\n".join(f"  {i+1}. <code>{escape_html(u)}</code>" for i, u in enumerate(urls))
    texto = f"🔀 <b>{len(urls)} lista(s) seleccionadas</b>\n\n{lista_txt}\n\nSelecciona qué tipos incluir:"
    if editar:
        try:
            await update.callback_query.message.edit_text(texto, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
            return
        except Exception:
            pass
    messages = await send_long_message(update, context, texto, reply_markup=InlineKeyboardMarkup(keyboard))
    context.bot_data['chat_data'][chat_id]['pending_messages'] = messages

async def fusionar_listas(update, context):
    chat_id = update.effective_chat.id
    ensure_chat_data(context, chat_id)
    urls = context.bot_data['chat_data'][chat_id].get('fusion_urls', [])
    tipos = context.bot_data['chat_data'][chat_id].get('fusion_tipos', [])
    if len(urls) < 2:
        await send_long_message(update, context, "❌ Necesitas al menos 2 listas.")
        return
    if not tipos:
        await send_long_message(update, context, "❌ Selecciona al menos un tipo.")
        return
    status_msg = await update.callback_query.message.reply_text(f"🔀 Fusionando {len(urls)} listas... Por favor espera...")
    try:
        all_streams_per_list = []
        for i, url in enumerate(urls):
            await status_msg.edit_text(f"🔀 Obteniendo streams de lista {i+1}/{len(urls)}...")
            p = urllib.parse.urlparse(url)
            qp = urllib.parse.parse_qs(p.query)
            host = f"{p.scheme}://{p.netloc}"
            user = qp.get('username', [''])[0]
            pwd = qp.get('password', [''])[0]
            api = f"{host}/player_api.php?username={user}&password={pwd}"
            all_streams_per_list.append(await _get_streams_completos(url, host, user, pwd, api, tipos))
        await status_msg.edit_text("⚙️ Deduplicando y evaluando calidad...")
        candidatos_por_tipo = {t: {} for t in tipos}
        for streams_lista in all_streams_per_list:
            for tipo in tipos:
                for item in streams_lista.get(tipo, []):
                    norm = item['name'].strip().lower()
                    candidatos_por_tipo[tipo].setdefault(norm, []).append(item)
        links_a_medir = [item['link'] for tipo in tipos for norm, items in candidatos_por_tipo[tipo].items() if len(items) > 1 for item in items]
        await status_msg.edit_text(f"⚡ Midiendo velocidad de {len(links_a_medir)} streams duplicados...")
        velocidades = {}
        if links_a_medir:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=6)) as session:
                velocidades = dict(zip(links_a_medir, await asyncio.gather(*[_medir_velocidad(session, lnk) for lnk in links_a_medir])))
        await status_msg.edit_text("🏆 Seleccionando el mejor stream...")
        seleccionados = {t: [] for t in tipos}
        for tipo in tipos:
            for norm, items in candidatos_por_tipo[tipo].items():
                if len(items) == 1:
                    seleccionados[tipo].append(items[0])
                else:
                    seleccionados[tipo].append(max(items, key=lambda item: _score_calidad(item['name']) * 1000 - velocidades.get(item['link'], 9999.0)))
        await status_msg.edit_text("📄 Generando archivo M3U...")
        total = sum(len(seleccionados[t]) for t in tipos)
        tipo_label = {'channels': '📺 Live TV', 'series': '📽️ Series', 'movies': '🎬 Películas'}
        lineas = ["#EXTM3U", f"# Fusión JOX3 IPTV — {datetime.now().strftime('%Y-%m-%d %H:%M')}", f"# Total: {total}", ""]
        for tipo in tipos:
            lineas.append(f"# ══ {tipo_label.get(tipo, tipo)} ({len(seleccionados[tipo])}) ══")
            for item in sorted(seleccionados[tipo], key=lambda x: (x['category'], x['name'])):
                lineas.append(f'#EXTINF:-1 group-title="{item["category"]}",{item["name"]}')
                lineas.append(item['link'])
            lineas.append("")
        buf = BytesIO()
        buf.write("\n".join(lineas).encode('utf-8'))
        buf.seek(0)
        await status_msg.delete()
        resumen = [f"🔀 <b>Fusión completada</b> — {len(urls)} listas\n"]
        for tipo in tipos:
            resumen.append(f"  {tipo_label.get(tipo,'?')}: <b>{len(seleccionados[tipo]):,}</b> streams")
        resumen.append(f"\n📊 Total: {total:,} · {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        messages = await send_long_message(update, context, "\n".join(resumen))
        context.bot_data['chat_data'][chat_id]['pending_messages'] = messages
        await update.callback_query.message.reply_document(document=buf, filename="fusion_jox3.m3u", caption=f"✅ Lista fusionada — {total:,} streams únicos.")
        context.bot_data['chat_data'][chat_id]['fusion_urls'] = []
        context.bot_data['chat_data'][chat_id]['fusion_tipos'] = []
    except Exception as e:
        logger.error(f"Error fusionar_listas: {e}", exc_info=True)
        try:
            await status_msg.delete()
        except Exception:
            pass
        messages = await send_long_message(update, context, f"⚠️ Error: {escape_html(str(e))}")
        context.bot_data['chat_data'][chat_id]['pending_messages'] = messages

async def cmd_fusionar(update, context):
    chat_id = update.effective_chat.id
    ensure_chat_data(context, chat_id)
    context.bot_data['chat_data'][chat_id]['fusion_urls'] = []
    context.bot_data['chat_data'][chat_id]['fusion_tipos'] = []
    context.bot_data['chat_data'][chat_id]['awaiting_fusion_url'] = True
    messages = await send_long_message(update, context, "🔀 <b>Fusionador de listas M3U</b>\n\nEnvía la URL de la <b>Lista 1</b>:")
    context.bot_data['chat_data'][chat_id]['pending_messages'] = messages

def main():
    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("info", info))
    application.add_handler(CommandHandler("check", check_list))
    application.add_handler(CommandHandler("comparar", cmd_comparar))
    application.add_handler(CommandHandler("fusionar", cmd_fusionar))
    application.add_handler(CallbackQueryHandler(handle_category_selection))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    application.add_handler(MessageHandler(filters.Document.MimeType("text/plain"), handle_document))
    logger.info("Bot iniciado")
    application.run_polling(allowed_updates=["message", "callback_query", "edited_message", "channel_post", "edited_channel_post", "inline_query", "chosen_inline_result", "chat_member", "my_chat_member", "chat_join_request"])

if __name__ == '__main__':
    main()
