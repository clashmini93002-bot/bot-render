import os
import zipfile
import asyncio
from pathlib import Path
import requests
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode
import py7zr
from PIL import Image
import io
import tarfile
import base64
from bs4 import BeautifulSoup
import time
from flask import Flask, request, jsonify
import threading
import schedule

# ---------------------------
# Config desde Variables de Entorno
# ---------------------------
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
IMGBB_API_KEY = os.environ.get("IMGBB_API_KEY", "")
TELEGRAPH_TOKEN = os.environ.get("TELEGRAPH_TOKEN", "")
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "")

# ---------------------------
# Configuración de Flask para Webhook
# ---------------------------
web_app = Flask(__name__)

# ---------------------------
# Ajustes operativos
# ---------------------------
ZIP_DISK_LIMIT = 900 * 1024 * 1024
TMP_DIR = "tmp_work"
MAX_IMAGE_DIM = 1280
JPEG_QUALITY = 85

app = Client(
    "smart-zip-bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True  # Importante para Render
)

# ---------------------------
# Estado global
# ---------------------------
last_post = {}  # Guarda {"path":..., "img_urls":[...], "final_url":...}
current_imgbb_keys = []  # Lista de API keys disponibles
current_key_index = 0  # Índice de la key actualmente en uso
valid_keys = set()  # Keys que han funcionado correctamente
failed_keys = set()  # Keys que han fallado
pending_key_requests = {}  # {chat_id: {"status_msg": Message, "image_sources": list, "current_index": int, "processed_links": list, "errors": list}}
active_listeners = {}  # {chat_id: {"event": asyncio.Event, "response": str}}
ping_counter = 0
start_time = time.time()

# ---------------------------
# Utilidades
# ---------------------------

def is_image_filename(name: str) -> bool:
    return name.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".webp"))

def is_safe_member_name(name: str) -> bool:
    if not name:
        return False
    if name.startswith("/") or name.startswith("\\"):
        return False
    if ".." in Path(name).parts:
        return False
    if ':' in name and (len(name) >= 2 and name[1] == ':'):
        return False
    if "\\" in name:
        return False
    return True

async def validar_y_reparar_imagen(path_or_bytes):
    try:
        if isinstance(path_or_bytes, (bytes, bytearray)):
            buffer_in = io.BytesIO(path_or_bytes)
        elif isinstance(path_or_bytes, io.BytesIO):
            buffer_in = path_or_bytes
            buffer_in.seek(0)
        else:
            buffer_in = open(path_or_bytes, "rb")

        with Image.open(buffer_in) as img:
            img = img.convert("RGB")
            w, h = img.size
            if w > MAX_IMAGE_DIM or h > MAX_IMAGE_DIM:
                img.thumbnail((MAX_IMAGE_DIM, MAX_IMAGE_DIM))
            buffer_out = io.BytesIO()
            img.save(buffer_out, format="JPEG", quality=JPEG_QUALITY, optimize=True)
            buffer_out.seek(0)

        if not isinstance(path_or_bytes, (bytes, bytearray, io.BytesIO)):
            buffer_in.close()

        return buffer_out
    except Exception as e:
        print(f"Error validando/optimizando imagen: {e}")
        return None

def upload_to_imgbb(image_buffer: io.BytesIO, name: str = None, api_key: str = None, retries: int = 3, delay_retry: float = 1.0) -> dict:
    """
    Sube una imagen a ImgBB con reintentos en caso de timeout.
    Retorna un dict: {"success": bool, "url": str, "error": str, "key_valid": bool}
    """
    if not api_key:
        return {"success": False, "error": "No API key provided", "key_valid": False}

    encoded_image = None
    if image_buffer:
        try:
            image_buffer.seek(0)
            encoded_image = base64.b64encode(image_buffer.read()).decode("utf-8")
        except Exception as e:
            return {"success": False, "error": f"Error leyendo buffer: {e}", "key_valid": True}

    url = "https://api.imgbb.com/1/upload"
    payload = {"key": api_key, "image": encoded_image}
    if name:
        payload["name"] = Path(name).stem[:100]

    for attempt in range(1, retries + 1):
        try:
            r = requests.post(url, data=payload, timeout=30)
            
            # Verificar si la API key es inválida o ha excedido el límite
            if r.status_code == 400:
                try:
                    error_data = r.json()
                    error_message = error_data.get("error", {}).get("message", "").lower()
                    if any(word in error_message for word in ["key", "invalid", "expired", "missing"]):
                        return {"success": False, "error": "API key inválida o expirada", "key_valid": False}
                except:
                    pass
            
            r.raise_for_status()
            resp = r.json()
            if resp.get("success") and "data" in resp and "url" in resp["data"]:
                return {"success": True, "url": resp["data"]["url"], "key_valid": True}
            return {"success": False, "error": f"Error en upload: {resp}", "key_valid": True}
        except requests.exceptions.ReadTimeout:
            if attempt < retries:
                print(f"⚠️ Timeout en subida, reintentando ({attempt}/{retries})...")
                time.sleep(delay_retry)
                continue
            else:
                return {"success": False, "error": "Timeout agotado tras varios intentos", "key_valid": True}
        except requests.exceptions.HTTPError as e:
            if r.status_code == 400:
                return {"success": False, "error": f"HTTP Error: {e}", "key_valid": False}
            return {"success": False, "error": f"HTTP Error: {e}", "key_valid": True}
        except Exception as e:
            return {"success": False, "error": f"Error subiendo imagen: {e}", "key_valid": True}

    return {"success": False, "error": "Error desconocido", "key_valid": True}

def initialize_imgbb_keys():
    """Inicializa la lista de API keys desde la variable de entorno"""
    global current_imgbb_keys, current_key_index, valid_keys, failed_keys
    if IMGBB_API_KEY:
        # Permitir múltiples keys separadas por coma
        keys = [key.strip() for key in IMGBB_API_KEY.split(",") if key.strip()]
        current_imgbb_keys = keys
        current_key_index = 0
        valid_keys = set(keys)  # Inicialmente asumimos que todas son válidas
        failed_keys = set()
        print(f"✅ Inicializadas {len(keys)} API keys de ImgBB")
    else:
        current_imgbb_keys = []
        valid_keys = set()
        failed_keys = set()
        print("⚠️ No se encontraron API keys de ImgBB")

def get_current_imgbb_key() -> str:
    """Obtiene la API key actualmente en uso"""
    global current_key_index
    
    if not current_imgbb_keys:
        return ""
    
    # Si tenemos keys válidas, usar una de ellas
    available_valid_keys = [key for key in current_imgbb_keys if key in valid_keys and key not in failed_keys]
    if available_valid_keys:
        return available_valid_keys[0]
    
    # Si no hay keys válidas, usar la siguiente key disponible
    if current_key_index >= len(current_imgbb_keys):
        current_key_index = 0
    
    return current_imgbb_keys[current_key_index] if current_imgbb_keys else ""

def mark_key_as_valid(key: str):
    """Marca una key como válida"""
    valid_keys.add(key)
    if key in failed_keys:
        failed_keys.remove(key)
    print(f"✅ Key marcada como válida: {key[:10]}...")

def mark_key_as_failed(key: str):
    """Marca una key como fallida"""
    failed_keys.add(key)
    if key in valid_keys:
        valid_keys.remove(key)
    print(f"❌ Key marcada como fallida: {key[:10]}...")

def rotate_to_next_key():
    """Rota a la siguiente key disponible"""
    global current_key_index
    
    if not current_imgbb_keys:
        return
    
    current_key_index = (current_key_index + 1) % len(current_imgbb_keys)
    print(f"🔄 Rotando a siguiente key. Índice actual: {current_key_index}")

async def request_new_key(client: Client, chat_id: int, status_msg: Message) -> str:
    """Solicita una nueva API key al usuario"""
    await status_msg.edit_text("🔑 **Se necesita una nueva API key de ImgBB**\n\nPor favor, envía la nueva API key o escribe /cancel para cancelar la operación.")
    
    # Crear un evento para esperar la respuesta
    event = asyncio.Event()
    active_listeners[chat_id] = {"event": event, "response": None}
    
    try:
        # Esperar máximo 5 minutos
        await asyncio.wait_for(event.wait(), timeout=300)
        
        # Obtener la respuesta
        response_data = active_listeners.get(chat_id, {})
        response_text = response_data.get("response")
        
        if response_text and response_text.lower() == "/cancel":
            return None
        
        if response_text and len(response_text) >= 10:
            return response_text
        else:
            await client.send_message(chat_id, "⚠️ API key inválida. Por favor, envía una key válida.")
            return await request_new_key(client, chat_id, status_msg)
            
    except asyncio.TimeoutError:
        await client.send_message(chat_id, "❌ Tiempo de espera agotado. Operación cancelada.")
        return None
    finally:
        # Limpiar el listener
        if chat_id in active_listeners:
            del active_listeners[chat_id]

# ---------------------------
# Procesamiento con manejo dinámico de API keys
# ---------------------------
async def process_and_upload_images(image_sources: list, client: Client = None, chat_id: int = None, status_msg=None):
    image_sources.sort(key=lambda x: x[0].lower())
    links = []
    errors = []
    
    if not current_imgbb_keys:
        await status_msg.edit_text("❌ No hay API keys configuradas. Usa /addkey para agregar una.")
        return links, errors

    current_key = get_current_imgbb_key()
    if not current_key:
        await status_msg.edit_text("❌ No hay API keys válidas disponibles.")
        return links, errors

    total = len(image_sources)
    idx = 0
    consecutive_successes = 0  # Contador de éxitos consecutivos
    
    while idx < total:
        name, payload = image_sources[idx]
        idx += 1
        
        # Procesar imagen
        buf = await validar_y_reparar_imagen(payload)
        if buf is None:
            errors.append(f"No pude procesar {name}")
            continue
        
        # Intentar subir con la key actual
        result = upload_to_imgbb(buf, name=name, api_key=current_key)
        
        if result["success"]:
            links.append((name, result["url"]))
            consecutive_successes += 1
            # Si tenemos varios éxitos consecutivos, marcamos la key como válida
            if consecutive_successes >= 3:
                mark_key_as_valid(current_key)
        else:
            consecutive_successes = 0  # Resetear contador de éxitos
            
            if not result["key_valid"]:
                # Key inválida, necesitamos una nueva
                print(f"⚠️ Key inválida detectada para {name}. Solicitando nueva key...")
                mark_key_as_failed(current_key)
                
                # Guardar estado actual
                pending_state = {
                    "status_msg": status_msg,
                    "image_sources": image_sources,
                    "current_index": idx - 1,  # Volver a intentar esta imagen
                    "processed_links": links.copy(),
                    "errors": errors.copy(),
                    "chat_id": chat_id
                }
                pending_key_requests[chat_id] = pending_state
                
                # Solicitar nueva key
                new_key = await request_new_key(client, chat_id, status_msg)
                
                if new_key is None:
                    # Usuario canceló
                    print("❌ Usuario canceló la operación.")
                    return links, errors
                
                # Agregar la nueva key a la lista
                current_imgbb_keys.append(new_key)
                current_key = new_key
                mark_key_as_valid(new_key)  # Asumimos que la nueva key es válida
                print(f"✅ Nueva key agregada. Total de keys: {len(current_imgbb_keys)}")
                
                # Reintentar la misma imagen con la nueva key
                idx -= 1
                continue
            else:
                # Otro tipo de error (no relacionado con la key)
                errors.append(f"{name}: {result['error']}")
        
        # Actualizar progreso
        if status_msg and client and chat_id:
            progress = int((idx / total) * 20)
            bar = "█" * progress + "░" * (20 - progress)
            text = f"🖼️ Procesando imágenes...\n[{bar}] {int((idx/total)*100)}% ({idx}/{total})"
            try:
                await status_msg.edit_text(text)
            except Exception as e:
                print(f"Error editando mensaje: {e}")
        
        await asyncio.sleep(0.5)
    
    # Si llegamos al final sin errores de key, marcamos como válida
    if consecutive_successes > 0:
        mark_key_as_valid(current_key)
    
    return links, errors

async def resume_after_key_update(client: Client, chat_id: int):
    """Reanuda el procesamiento después de actualizar la API key"""
    if chat_id not in pending_key_requests:
        return None, None
    
    state = pending_key_requests[chat_id]
    status_msg = state["status_msg"]
    image_sources = state["image_sources"]
    current_index = state["current_index"]
    links = state["processed_links"]
    errors = state["errors"]
    
    # Eliminar el estado pendiente
    del pending_key_requests[chat_id]
    
    if not current_imgbb_keys:
        await status_msg.edit_text("❌ No hay API keys configuradas.")
        return links, errors
    
    current_key = get_current_imgbb_key()
    if not current_key:
        await status_msg.edit_text("❌ No hay API keys válidas disponibles.")
        return links, errors
    
    # Continuar con el procesamiento desde donde se quedó
    total = len(image_sources)
    idx = current_index
    consecutive_successes = 0
    
    await status_msg.edit_text(f"🔄 Reanudando procesamiento desde imagen {idx + 1} de {total}...")
    
    while idx < total:
        name, payload = image_sources[idx]
        idx += 1
        
        buf = await validar_y_reparar_imagen(payload)
        if buf is None:
            errors.append(f"No pude procesar {name}")
            continue
        
        result = upload_to_imgbb(buf, name=name, api_key=current_key)
        
        if result["success"]:
            links.append((name, result["url"]))
            consecutive_successes += 1
            if consecutive_successes >= 3:
                mark_key_as_valid(current_key)
        else:
            consecutive_successes = 0
            
            if not result["key_valid"]:
                # Key aún inválida, volver a solicitar
                print(f"⚠️ Key aún inválida para {name}. Solicitando nueva key...")
                mark_key_as_failed(current_key)
                
                pending_state = {
                    "status_msg": status_msg,
                    "image_sources": image_sources,
                    "current_index": idx - 1,
                    "processed_links": links.copy(),
                    "errors": errors.copy(),
                    "chat_id": chat_id
                }
                pending_key_requests[chat_id] = pending_state
                
                new_key = await request_new_key(client, chat_id, status_msg)
                
                if new_key is None:
                    return links, errors
                
                current_imgbb_keys.append(new_key)
                current_key = new_key
                mark_key_as_valid(new_key)
                print(f"✅ Nueva key agregada. Total de keys: {len(current_imgbb_keys)}")
                idx -= 1
                continue
            else:
                errors.append(f"{name}: {result['error']}")
        
        # Actualizar progreso
        progress = int((idx / total) * 20)
        bar = "█" * progress + "░" * (20 - progress)
        text = f"🖼️ Procesando imágenes...\n[{bar}] {int((idx/total)*100)}% ({idx}/{total})"
        try:
            await status_msg.edit_text(text)
        except Exception as e:
            print(f"Error editando mensaje: {e}")
        
        await asyncio.sleep(0.5)
    
    if consecutive_successes > 0:
        mark_key_as_valid(current_key)
    
    return links, errors

# ---------------------------
# Telegraph
# ---------------------------
def create_telegraph_post(title: str, image_links: list) -> str:
    if not TELEGRAPH_TOKEN:
        return "TELEGRAPH_TOKEN no configurado en Secrets."

    content_nodes = [{"tag": "img", "attrs": {"src": link}} for link in image_links]

    url = "https://api.telegra.ph/createPage"
    payload = {
        "access_token": TELEGRAPH_TOKEN,
        "title": title,
        "content": str(content_nodes).replace("'", '"'),
        "author_name": "ImgBot"
    }

    try:
        r = requests.post(url, data=payload, timeout=30)
        r.raise_for_status()
        resp = r.json()
        if resp.get("ok"):
            return resp["result"]["url"]
        else:
            return f"Error creando post: {resp}"
    except Exception as e:
        return f"Error conectando a Telegraph: {e}"

def extract_telegra_path_from_url(url: str) -> str:
    try:
        if not isinstance(url, str):
            return ""
        part = url.split("://", 1)[-1]
        path = part.split("/", 1)[1]
        path = path.split("?", 1)[0].split("#", 1)[0]
        return path.strip("/")
    except Exception:
        return ""

def edit_telegraph_title_only(path: str, new_title: str, image_links: list) -> str:
    if not TELEGRAPH_TOKEN:
        return "TELEGRAPH_TOKEN no configurado en Secrets."
    if not path:
        return "Ruta de Telegraph inválida."

    content_nodes = [{"tag": "img", "attrs": {"src": link}} for link in image_links]

    url = "https://api.telegra.ph/editPage"
    payload = {
        "access_token": TELEGRAPH_TOKEN,
        "path": path,
        "title": new_title,
        "content": str(content_nodes).replace("'", '"'),
        "author_name": "♀ Asian My Waifu Cosplay ♀"
    }

    try:
        r = requests.post(url, data=payload, timeout=30)
        r.raise_for_status()
        resp = r.json()
        if resp.get("ok"):
            return resp["result"]["url"]
        else:
            return f"Error editando post: {resp}"
    except Exception as e:
        return f"Error conectando a Telegraph (edit): {e}"

def edit_telegraph_with_description(path: str, title: str, image_links: list, description: str) -> str:
    if not TELEGRAPH_TOKEN:
        return "TELEGRAPH_TOKEN no configurado en Secrets."

    content_nodes = []
    if image_links:
        content_nodes.append({"tag": "img", "attrs": {"src": image_links[0]}})
        if description:
            content_nodes.append({"tag": "p", "children": [description]})
        for link in image_links[1:]:
            content_nodes.append({"tag": "img", "attrs": {"src": link}})

    url = "https://api.telegra.ph/editPage"
    payload = {
        "access_token": TELEGRAPH_TOKEN,
        "path": path,
        "title": title,
        "content": str(content_nodes).replace("'", '"'),
        "author_name": "♀️ Asian My Waifu Cosplay ♀️"
    }

    try:
        r = requests.post(url, data=payload, timeout=30)
        r.raise_for_status()
        resp = r.json()
        if resp.get("ok"):
            return resp["result"]["url"]
        else:
            return f"Error editando con descripción: {resp}"
    except Exception as e:
        return f"Error conectando a Telegraph (desc): {e}"

# ---------------------------
# Manejo de contenedores
# ---------------------------
async def after_post_created(client: Client, message: Message, file_path: str, final_url: str, img_urls: list, path: str):
    chat_id = message.chat.id
    last_post.clear()
    last_post.update({
        "path": path,
        "img_urls": img_urls,
        "final_url": final_url,
        "title": Path(file_path).stem,
        "chat_id": chat_id
    })
    await client.send_message(chat_id, f"✅ Post creado en Telegraph:\n{final_url}\n\n¿Quieres añadir una descripción entre la primera y segunda imagen?\nResponde con el texto o escribe /skip para omitir.")

async def handle_container(client: Client, message: Message, file_path: str, container_type: str):
    chat_id = message.chat.id
    status_msg = await client.send_message(chat_id, f"⏳ Procesando {container_type}...")

    try:
        # Limpiar cualquier estado pendiente previo
        if chat_id in pending_key_requests:
            del pending_key_requests[chat_id]
        if chat_id in active_listeners:
            del active_listeners[chat_id]

        if container_type == "ZIP":
            with zipfile.ZipFile(file_path, "r") as zf:
                members = [m for m in zf.namelist() if not m.endswith("/")]
                image_sources = []
                for member in members:
                    if not is_safe_member_name(member):
                        continue
                    try:
                        with zf.open(member) as fh:
                            data = fh.read()
                        if is_image_filename(member):
                            image_sources.append((member, data))
                    except Exception as e:
                        await client.send_message(chat_id, f"⚠️ Error leyendo {member}: {e}")

        elif container_type == "7Z":
            with py7zr.SevenZipFile(file_path, mode='r') as archive:
                allnames = archive.getnames()
                image_sources = []
                for member in allnames:
                    if not is_safe_member_name(member):
                        continue
                    try:
                        read_dict = archive.read([member])
                        if member in read_dict:
                            data = read_dict[member].read()
                            if is_image_filename(member):
                                image_sources.append((member, data))
                    except Exception as e:
                        await client.send_message(chat_id, f"⚠️ Error al extraer {member}: {e}")

        elif container_type == "TAR":
            with tarfile.open(file_path, 'r:*') as tar:
                members = [m for m in tar.getmembers() if m.isreg()]
                image_sources = []
                for m in members:
                    name = m.name
                    if not is_safe_member_name(name):
                        continue
                    f = tar.extractfile(m)
                    if f and is_image_filename(name):
                        image_sources.append((name, f.read()))
        else:
            await status_msg.edit_text("❌ Formato no soportado.")
            return

        if not image_sources:
            await status_msg.edit_text("⚠️ No se encontraron imágenes válidas.")
            return

        await status_msg.edit_text("🖼️ Procesando imágenes...")
        
        # Verificar si hay una operación pendiente para este chat
        if chat_id in pending_key_requests:
            links, errs = await resume_after_key_update(client, chat_id)
        else:
            links, errs = await process_and_upload_images(image_sources, client, chat_id, status_msg)
        
        if links is None:  # Operación cancelada
            await status_msg.edit_text("❌ Operación cancelada por el usuario.")
            return
            
        img_urls = [link for _, link in links]

        if not img_urls:
            await status_msg.edit_text("❌ No se pudieron subir imágenes.")
            return

        await status_msg.edit_text("🔗 Creando publicación en Telegraph...")
        original_url = create_telegraph_post("Galería de imágenes", img_urls)
        final_url = original_url
        path = ""
        if isinstance(original_url, str) and original_url.startswith("http"):
            title = Path(file_path).stem
            path = extract_telegra_path_from_url(original_url)
            edited_url = edit_telegraph_title_only(path, title, img_urls)
            if isinstance(edited_url, str) and edited_url.startswith("http"):
                final_url = edited_url

        # Limpiar estados pendientes antes de continuar
        if chat_id in pending_key_requests:
            del pending_key_requests[chat_id]
        if chat_id in active_listeners:
            del active_listeners[chat_id]

        await after_post_created(client, message, file_path, final_url, img_urls, path)

        if errs:
            # Enviar errores en chunks para evitar mensajes muy largos
            error_chunks = [errs[i:i+5] for i in range(0, len(errs), 5)]
            for chunk in error_chunks:
                await client.send_message(chat_id, "\n".join([f"⚠️ {err}" for err in chunk]))

        await status_msg.delete()
        
    except Exception as e:
        await status_msg.edit_text(f"❌ Error procesando archivo: {e}")
        print(f"Error en handle_container: {e}")
    finally:
        try:
            os.remove(file_path)
        except:
            pass

# ---------------------------
# Comando /ul
# ---------------------------
@app.on_message(filters.command("ul") & filters.reply)
async def cmd_ul(client: Client, message: Message):
    reply = message.reply_to_message
    chat_id = message.chat.id

    if not reply:
        await client.send_message(chat_id, "⚠️ Responde al mensaje con el archivo usando /ul.")
        return

    doc = reply.document or None
    if not doc or not getattr(doc, "file_name", None):
        await client.send_message(chat_id, "⚠️ Archivo no válido. Usa ZIP/7z/TAR.")
        return

    filename = doc.file_name
    lower = filename.lower()

    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
    download_target = str(Path(TMP_DIR) / filename)

    status_msg = await client.send_message(chat_id, f"📥 Descargando archivo `{filename}`...")
    try:
        file_path = await client.download_media(reply, file_name=download_target)
        await status_msg.edit_text(f"✅ Archivo descargado: `{filename}`")
    except Exception as e:
        await status_msg.edit_text(f"❌ Error descargando: {e}")
        return

    if lower.endswith(".zip"):
        await handle_container(client, message, file_path, "ZIP")
    elif lower.endswith(".7z"):
        await handle_container(client, message, file_path, "7Z")
    elif lower.endswith(('.tar', '.tar.gz', '.tgz')):
        await handle_container(client, message, file_path, "TAR")
    else:
        await status_msg.edit_text("❌ Formato no soportado.")

# ---------------------------
# Manejo de mensajes de texto
# ---------------------------
@app.on_message(filters.text & ~filters.command(["ul", "skip", "keys", "reset"]))
async def handle_text_messages(client: Client, message: Message):
    chat_id = message.chat.id
    text = message.text.strip()
    
    print(f"Mensaje recibido en chat {chat_id}: {text[:50]}...")
    
    # Primero verificar si hay un listener activo para una nueva key
    if chat_id in active_listeners:
        print(f"Listener activo encontrado para chat {chat_id}")
        listener_data = active_listeners[chat_id]
        
        # Si es un comando /cancel
        if text.lower() == "/cancel":
            print("Comando /cancel detectado")
            listener_data["response"] = "/cancel"
            listener_data["event"].set()
            return
        
        # Si es un comando /addkey, procesarlo
        if text.startswith("/addkey"):
            parts = text.split()
            if len(parts) >= 2:
                new_key = parts[1].strip()
                if len(new_key) >= 10:
                    current_imgbb_keys.append(new_key)
                    mark_key_as_valid(new_key)
                    await message.reply(f"✅ API key agregada. Total de keys: {len(current_imgbb_keys)}")
                    # También establecer esta key como respuesta para el listener
                    listener_data["response"] = new_key
                    listener_data["event"].set()
                    print(f"Nueva key agregada via /addkey: {new_key[:10]}...")
                else:
                    await message.reply("⚠️ API key inválida. Debe tener al menos 10 caracteres.")
            else:
                await message.reply("⚠️ Uso: /addkey <api_key>")
            return
        
        # Si es texto normal, asumir que es una API key
        if len(text) >= 10:
            listener_data["response"] = text
            listener_data["event"].set()
            await message.reply("✅ API key recibida. Continuando...")
            print(f"API key recibida: {text[:10]}...")
        else:
            await message.reply("⚠️ API key inválida. Debe tener al menos 10 caracteres.")
        return
    
    # Si no hay listener activo, manejar la descripción del post
    if last_post and last_post.get("chat_id") == chat_id:
        print(f"Manejando descripción para post en chat {chat_id}")
        description = text
        path = last_post.get("path")
        title = last_post.get("title")
        img_urls = last_post.get("img_urls", [])
        edited_url = edit_telegraph_with_description(path, title, img_urls, description)
        await client.send_message(chat_id, f"✍️ Descripción añadida:\n{edited_url}")
        last_post.clear()
        return
    
    print(f"No se procesó el mensaje en chat {chat_id}")

@app.on_message(filters.command("skip"))
async def skip_description(client: Client, message: Message):
    chat_id = message.chat.id
    if last_post and last_post.get("chat_id") == chat_id:
        link = last_post.get("final_url")
        try:
            res = requests.get(link, timeout=10)
            soup = BeautifulSoup(res.text, "html.parser")
            title = soup.find("h1").get_text(strip=True) if soup.find("h1") else "Sin título"
            first_part = title.split("-")[0].strip()
            image_count = len(soup.find_all("img"))
            summary = f"♀️ [{first_part}]({link}) ♀️\nAlbum ({image_count} Pictures)"
        except:
            summary = link
        await client.send_message(chat_id, summary, parse_mode="Markdown")
        last_post.clear()

@app.on_message(filters.command("addkey"))
async def add_key_manual(client: Client, message: Message):
    chat_id = message.chat.id
    if len(message.command) < 2:
        await client.send_message(chat_id, "⚠️ Uso: /addkey <api_key>")
        return
    
    new_key = message.command[1].strip()
    if len(new_key) < 10:
        await client.send_message(chat_id, "⚠️ API key inválida. Debe tener al menos 10 caracteres.")
        return
    
    current_imgbb_keys.append(new_key)
    mark_key_as_valid(new_key)
    await client.send_message(chat_id, f"✅ API key agregada. Total de keys: {len(current_imgbb_keys)}")

@app.on_message(filters.command("keys"))
async def show_keys(client: Client, message: Message):
    chat_id = message.chat.id
    key_count = len(current_imgbb_keys)
    valid_count = len(valid_keys)
    failed_count = len(failed_keys)
    
    keys_info = f"🔑 **Estado de API Keys:**\n"
    keys_info += f"• Total: {key_count}\n"
    keys_info += f"• Válidas: {valid_count}\n"
    keys_info += f"• Fallidas: {failed_count}\n"
    
    if current_imgbb_keys:
        current_key = get_current_imgbb_key()
        keys_info += f"• Key actual: `{current_key[:15]}...`"
    else:
        keys_info += "⚠️ No hay keys configuradas."
    
    await client.send_message(chat_id, keys_info)

@app.on_message(filters.command("reset"))
async def reset_state(client: Client, message: Message):
    chat_id = message.chat.id
    # Limpiar todos los estados
    if chat_id in pending_key_requests:
        del pending_key_requests[chat_id]
    if chat_id in active_listeners:
        del active_listeners[chat_id]
    if last_post and last_post.get("chat_id") == chat_id:
        last_post.clear()
    await client.send_message(chat_id, "✅ Estado resetado. Puedes intentar de nuevo.")

@app.on_message(filters.command("status"))
async def bot_status(client: Client, message: Message):
    chat_id = message.chat.id
    status_info = "🤖 **Estado del Bot:**\n"
    status_info += f"• Keys configuradas: {len(current_imgbb_keys)}\n"
    status_info += f"• Keys válidas: {len(valid_keys)}\n"
    status_info += f"• Instancia: {RENDER_INSTANCE_ID}\n"
    status_info += f"• Webhook: {'✅ Activo' if WEBHOOK_URL else '❌ Inactivo'}\n"
    status_info += "• Servidor: 🟢 En línea"
    
    await client.send_message(chat_id, status_info)

# ---------------------------
# Webhook Routes para Render
# ---------------------------
@web_app.route('/')
def home():
    uptime_minutes = (time.time() - start_time) / 60
    return jsonify({
        "status": "online",
        "service": "Telegram Smart ZIP Bot",
        "ping_count": ping_counter,
        "uptime_minutes": round(uptime_minutes, 1),
        "timestamp": time.time()
    })

@web_app.route('/health')
def health_check():
    return jsonify({
        "status": "healthy", 
        "ping_count": ping_counter,
        "timestamp": time.time()
    })

@web_app.route('/ping')
def ping():
    """Endpoint para self-ping"""
    return jsonify({
        "status": "pong", 
        "ping_count": ping_counter,
        "timestamp": time.time()
    })

@web_app.route('/status')
def status():
    """Endpoint detallado de estado"""
    uptime_minutes = (time.time() - start_time) / 60
    return jsonify({
        "service": "Telegram ZIP Bot",
        "status": "online",
        "uptime_minutes": round(uptime_minutes, 1),
        "ping_count": ping_counter,
        "keys_configured": len(current_imgbb_keys),
        "valid_keys": len(valid_keys),
        "timestamp": time.time()
    })

# ---------------------------
# Self-ping para mantener vivo el servicio
# ---------------------------
def self_ping():
    """Función para hacer self-ping cada 5 minutos"""
    global ping_counter
    if RENDER_EXTERNAL_URL:
        try:
            response = requests.get(f"{RENDER_EXTERNAL_URL}/ping", timeout=10)
            ping_counter += 1
            uptime_minutes = (time.time() - start_time) / 60
            print(f"✅ Ping #{ping_counter} - Status: {response.status_code} - Uptime: {uptime_minutes:.1f} min - {time.strftime('%H:%M:%S')}")
        except Exception as e:
            print(f"❌ Error en ping #{ping_counter}: {e}")

def start_scheduler():
    """Inicia el scheduler para self-ping"""
    print("🕐 Iniciando scheduler de self-ping (cada 5 minutos)...")
    
    # Primer ping inmediato
    self_ping()
    
    # Programar ping cada 5 minutos
    schedule.every(5).minutes.do(self_ping)
    
    while True:
        schedule.run_pending()
        time.sleep(1)
        
# ---------------------------
# Comando de estado mejorado
# ---------------------------
@app.on_message(filters.command("status"))
async def bot_status(client: Client, message: Message):
    chat_id = message.chat.id
    uptime_minutes = (time.time() - start_time) / 60
    
    status_info = "🤖 **Estado del Bot:**\n"
    status_info += f"• 🕐 Uptime: {uptime_minutes:.1f} minutos\n"
    status_info += f"• 📊 Pings realizados: {ping_counter}\n"
    status_info += f"• 🔑 Keys configuradas: {len(current_imgbb_keys)}\n"
    status_info += f"• ✅ Keys válidas: {len(valid_keys)}\n"
    status_info += f"• 🌐 URL: {RENDER_EXTERNAL_URL}\n"
    status_info += f"• 🚀 Estado: 🟢 **EN LÍNEA**\n"
    status_info += f"• ⏰ Último ping: {time.strftime('%H:%M:%S')}"
    
    await client.send_message(chat_id, status_info)

# ---------------------------
# Arranque mejorado para Render
# ---------------------------
def start_flask_app():
    """Inicia la aplicación Flask"""
    port = int(os.environ.get("PORT", 10000))
    print(f"🌐 Iniciando servidor Flask en puerto {port}...")
    web_app.run(host='0.0.0.0', port=port, debug=False, threaded=True)

async def main():
    """Función principal asincrónica"""
    initialize_imgbb_keys()
    
    print("🤖 Iniciando Telegram Bot...")
    print(f"🌐 URL Externa: {RENDER_EXTERNAL_URL}")
    
    try:
        await app.start()
        print("✅ Bot de Telegram iniciado correctamente")
        
        # Información del bot
        me = await app.get_me()
        print(f"🔗 Bot: @{me.username} ({me.first_name})")
        
        # Iniciar scheduler si hay URL configurada
        if RENDER_EXTERNAL_URL:
            scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
            scheduler_thread.start()
            print("✅ Scheduler de self-ping iniciado")
        else:
            print("⚠️ Self-ping desactivado (no hay URL configurada)")
        
        # Mantener el bot corriendo
        print("🚀 Bot completamente operativo")
        await asyncio.Event().wait()
        
    except Exception as e:
        print(f"❌ Error iniciando el bot: {e}")
    finally:
        await app.stop()

if __name__ == "__main__":
    from pathlib import Path
    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
    
    if not BOT_TOKEN:
        print("❌ BOT_TOKEN no configurado")
        exit(1)
    
    # Iniciar Flask en hilo separado
    flask_thread = threading.Thread(target=start_flask_app, daemon=True)
    flask_thread.start()
    
    # Ejecutar el bot
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(main())
        else:
            loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("👋 Bot detenido por el usuario")
    except Exception as e:
        print(f"❌ Error en el loop principal: {e}")
