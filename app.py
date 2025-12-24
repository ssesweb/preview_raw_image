import os
import subprocess
import json
import uuid
import time
import logging
import io
from PIL import Image, UnidentifiedImageError
from flask import Flask, render_template, request, jsonify, Response
from apscheduler.schedulers.background import BackgroundScheduler
from werkzeug.utils import secure_filename
from werkzeug.exceptions import RequestEntityTooLarge

# 配置日志（修复 StreamHandler 未定义问题）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('raw_parser.log'),
        logging.StreamHandler()  # 修正：添加 logging. 前缀
    ]
)

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB 限制
app.config['JSON_AS_ASCII'] = False

# 路径配置
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# 支持的RAW格式
ALLOWED_EXTENSIONS = {
    'nef', 'cr2', 'cr3', 'arw', 'sr2', 'srf', 'srw', 'dng', 
    'raf', 'pef', 'rw2', 'orf', '3fr', 'fff', 'iiq', 'mef'
}

# 定义通用/Web优化图片格式（无需转换）
SUPPORTED_WEB_FORMATS = {'JPEG', 'JPG', 'PNG', 'GIF', 'WebP', 'AVIF', 'SVG', 'BMP', 'ICO'}

# EXIF截断配置
MAX_LENGTH = 200
TRUNCATED_LENGTH = 40
ELLIPSIS = "......"

def allowed_file(filename):
    """校验文件格式是否允许"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def clean_old_files():
    """自动清理超过10分钟的文件"""
    try:
        now = time.time()
        cutoff = now - 600  # 10分钟
        files_removed = 0
        
        for f in os.listdir(UPLOAD_FOLDER):
            f_path = os.path.join(UPLOAD_FOLDER, f)
            if os.path.isfile(f_path) and os.stat(f_path).st_mtime < cutoff:
                os.remove(f_path)
                files_removed += 1
                logging.info(f"清理过期文件: {f}")
        
        if files_removed > 0:
            logging.info(f"共清理 {files_removed} 个过期文件")
    except Exception as e:
        logging.error(f"清理文件失败: {e}")

# 启动定时任务
scheduler = BackgroundScheduler()
# scheduler.add_job(func=clean_old_files, trigger="interval", seconds=300)  # 每5分钟清理一次
scheduler.start()

# ========== EXIF信息截断 ==========
def truncate_long_values(data):
    """递归遍历数据结构，截断过长的字符串值"""
    if isinstance(data, dict):
        for key, value in data.items():
            data[key] = truncate_long_values(value)
    elif isinstance(data, list):
        for i, item in enumerate(data):
            data[i] = truncate_long_values(item)
    elif isinstance(data, str):
        if len(data) > MAX_LENGTH:
            data = data[:TRUNCATED_LENGTH] + ELLIPSIS
    return data

# ========== 优先提取最大预览图（JpgFromRaw → PreviewImage） ==========
def extract_preview_data(filepath, tag_name=None):
    """
    提取预览图二进制数据，优先JpgFromRaw，再PreviewImage
    :param filepath: RAW文件路径
    :param tag_name: 指定标签（None则自动尝试）
    :return: (是否成功, 二进制数据, 使用的标签名)
    """
    # 优先尝试的标签列表
    priority_tags = ['JpgFromRaw', 'PreviewImage'] if tag_name is None else [tag_name]
    
    for tag in priority_tags:
        try:
            cmd = ["exiftool", "-b", f"-{tag}", filepath]
            result = subprocess.run(
                cmd, 
                check=True, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE
            )
            preview_data = result.stdout
            # 验证数据有效性（至少100字节）
            if preview_data and len(preview_data) > 100:
                logging.info(f"成功提取预览图（标签：{tag}），大小：{len(preview_data)/1024:.2f} KB")
                return True, preview_data, tag
            logging.warning(f"标签 {tag} 提取的数据无效（空或过小）")
        except subprocess.CalledProcessError as e:
            logging.warning(f"标签 {tag} 提取失败：{e.stderr.decode('utf-8', errors='ignore')}")
            continue
    
    return False, None, None

# ========== 复制原始EXIF到预览文件 ==========
def copy_exif_to_preview(source_raw_path, target_preview_path):
    """
    将原始RAW文件的EXIF元数据复制到预览文件
    :param source_raw_path: 原始RAW文件路径
    :param target_preview_path: 预览文件路径
    :return: 是否成功
    """
    try:
        cmd = [
            'exiftool',
            '-tagsfromfile', source_raw_path,
            '-all:all',
            '-unsafe',
            '-overwrite_original',
            target_preview_path
        ]
        result = subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8'
        )
        logging.info(f"EXIF元数据复制成功：{result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"EXIF复制失败：{e.stderr.strip()}")
        return False
    except Exception as e:
        logging.error(f"EXIF复制异常：{str(e)}")
        return False

# ========== 检测图片原始格式 ==========
def get_image_original_format(binary_data):
    """获取图片原始格式"""
    try:
        with Image.open(io.BytesIO(binary_data)) as img:
            return img.format.upper() if img.format else "UNKNOWN"
    except Exception:
        return "UNKNOWN"

# ========== 图片格式转换（非通用格式转JPG/WebP）【核心修改】 ==========
def convert_image_to_web_format(binary_data, target_format='JPEG', temp_file_prefix=None):
    """
    将非通用格式图片转换为Web兼容格式，并保留EXIF
    【修复】强制为所有预览图生成临时文件并复制EXIF，无论原始格式是否为通用格式
    :param binary_data: 原始二进制数据
    :param target_format: 目标格式（JPEG/WebP）
    :param temp_file_prefix: 临时文件前缀（用于复制EXIF）
    :return: (是否转换成功, 转换后二进制数据, 宽度, 高度, 原始格式)
    """
    # 先检测原始格式
    original_format = get_image_original_format(binary_data)
    logging.info(f"原始图片格式: {original_format}")
    
    # 生成唯一临时文件名（避免冲突）
    temp_file = None
    if temp_file_prefix:
        temp_file = f"{temp_file_prefix}_temp_{uuid.uuid4().hex[:8]}.jpg"
    
    try:
        with Image.open(io.BytesIO(binary_data)) as img:
            # 处理透明通道（JPEG不支持透明）
            if img.mode in ('RGBA', 'P', 'L'):
                img = img.convert('RGB')
            
            # 保存为目标格式到临时文件（无论是否通用格式）
            if temp_file:
                img.save(temp_file, format=target_format, quality=95)
                # 强制复制EXIF到临时文件
                copy_exif_to_preview(temp_file_prefix, temp_file)
                # 读取带EXIF的文件数据
                with open(temp_file, 'rb') as f:
                    converted_data = f.read()
            else:
                # 无临时文件前缀时直接返回转换后数据（无EXIF）
                output = io.BytesIO()
                img.save(output, format=target_format, quality=95)
                converted_data = output.getvalue()
            
            return True, converted_data, img.width, img.height, original_format
    except (UnidentifiedImageError, IOError, SyntaxError) as e:
        logging.error(f"图片转换失败: {e}")
        return False, None, 0, 0, original_format
    finally:
        # 确保临时文件被清理
        if temp_file and os.path.exists(temp_file):
            try:
                os.remove(temp_file)
                logging.info(f"清理临时文件: {temp_file}")
            except Exception as e:
                logging.warning(f"清理临时文件失败: {e}")

# ========== 核心函数：自动识别所有预览图二进制块 ==========
def get_preview_tags(raw_exif, filepath):
    """自动识别所有预览图二进制块"""
    valid_preview_tags = {}
    
    # 步骤1：遍历所有EXIF标签，筛选出包含二进制数据的标签
    for full_key in raw_exif.keys():
        tag_value = str(raw_exif.get(full_key, ""))
        if "(Binary data" not in tag_value:
            continue
        
        # 提取二进制块大小
        try:
            size_bytes = int(tag_value.split(" ")[2])
        except (IndexError, ValueError):
            continue
        
        # 筛选条件：10KB ~ 20MB
        if not (10240 <= size_bytes <= 20 * 1024 * 1024):
            continue
        
        # 提取标签名和完整组名
        tag_name = full_key.split(":")[-1] if ":" in full_key else full_key
        
        # 步骤2：提取二进制数据（优先JpgFromRaw）
        is_extract_success, binary_data, used_tag = extract_preview_data(filepath, tag_name)
        if not is_extract_success or not binary_data:
            continue
        
        # 转换为Web兼容格式，并复制EXIF
        is_success, converted_data, width, height, original_format = convert_image_to_web_format(
            binary_data, 
            target_format='JPEG', 
            temp_file_prefix=filepath  # 传入原始RAW路径，用于复制EXIF
        )
        
        if is_success and converted_data:
            valid_preview_tags[tag_name] = {
                "full_key": full_key,       # 完整标签
                "size_bytes": size_bytes,   # 原始大小
                "converted_size": len(converted_data),  # 转换后大小
                "width": width,
                "height": height,
                "original_format": original_format,     # 原始格式
                "converted_format": "JPEG" if original_format not in SUPPORTED_WEB_FORMATS else original_format,
                "used_tag": used_tag        # 实际使用的标签（JpgFromRaw/PreviewImage）
            }
    
    # 步骤3：按大小降序排序
    sorted_tags = sorted(
        valid_preview_tags.keys(),
        key=lambda x: valid_preview_tags[x]["size_bytes"],
        reverse=True
    )
    
    return sorted_tags, valid_preview_tags

# ========== 核心函数：提取原始EXIF（带截断） ==========
def get_raw_exif(filepath):
    """调用exiftool提取完整的原始EXIF数据（带截断）"""
    try:
        cmd = ["exiftool", "-j", "-a", "-G", "-n", filepath]
        result = subprocess.check_output(cmd, stderr=subprocess.PIPE).decode('utf-8', errors='ignore')
        exif_data = json.loads(result)[0] if result else {}
        
        # 截断过长的EXIF值
        exif_data = truncate_long_values(exif_data)
        
        return exif_data
    except subprocess.CalledProcessError as e:
        logging.error(f"EXIF提取失败: {e.stderr.decode('utf-8', errors='ignore')}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"EXIF JSON解析失败: {e}")
        return {}
    except Exception as e:
        logging.error(f"EXIF解析异常: {e}")
        return {}

# ========== 函数：解析常用EXIF字段 ==========
def parse_exif_for_display(raw_exif, original_filename):
    """从RAW EXIF中解析常用显示字段"""
    # 格式化文件大小
    file_size_bytes = raw_exif.get("System:FileSize") or raw_exif.get("File:FileSize")
    if file_size_bytes:
        if isinstance(file_size_bytes, str):
            try:
                file_size_bytes = int(file_size_bytes)
            except:
                file_size = "未知"
        else:
            if file_size_bytes < 1024:
                file_size = f"{file_size_bytes} B"
            elif file_size_bytes < 1024*1024:
                file_size = f"{file_size_bytes/1024:.2f} KB"
            else:
                file_size = f"{file_size_bytes/(1024*1024):.2f} MB"
    else:
        file_size = "未知"

    # 补充多厂商的字段兼容
    camera_model = raw_exif.get("IFD0:Model") or raw_exif.get("EXIF:Model") or raw_exif.get("MakerNotes:Model") or "未知"
    lens_model = raw_exif.get("Canon:LensModel") or raw_exif.get("EXIF:LensModel") or raw_exif.get("MakerNotes:LensModel") or raw_exif.get("Composite:LensID") or "未知"
    sensor_width = raw_exif.get("Canon:SensorWidth") or raw_exif.get("EXIF:SensorWidth") or 0
    sensor_height = raw_exif.get("Canon:SensorHeight") or raw_exif.get("EXIF:SensorHeight") or 0
    sensor_size = f"{sensor_width/1000:.2f} × {sensor_height/1000:.2f} mm" if sensor_width and sensor_height else "未知"

    return {
        "fileName": original_filename,
        "fileSize": file_size,
        "fileFormat": raw_exif.get("File:FileTypeExtension", "未知").upper(),
        "shootTime": raw_exif.get("Composite:SubSecDateTimeOriginal") or 
                     raw_exif.get("EXIF:DateTimeOriginal") or 
                     raw_exif.get("MakerNotes:DateTimeOriginal") or "未知",
        "cameraModel": camera_model,
        "lensModel": lens_model,
        "sensorSize": sensor_size,
        "resolution": {
            "width": raw_exif.get("EXIF:ImageWidth") or raw_exif.get("MakerNotes:ImageWidth") or "未知",
            "height": raw_exif.get("EXIF:ImageHeight") or raw_exif.get("MakerNotes:ImageHeight") or "未知"
        },
        "iso": raw_exif.get("Composite:ISO") or raw_exif.get("EXIF:ISO") or raw_exif.get("MakerNotes:ISO") or "未知",
        "shutterSpeed": raw_exif.get("Composite:ShutterSpeed") or raw_exif.get("EXIF:ExposureTime") or raw_exif.get("MakerNotes:ExposureTime") or "未知",
        "aperture": raw_exif.get("Composite:Aperture") or raw_exif.get("EXIF:FNumber") or raw_exif.get("MakerNotes:Aperture") or "未知",
        "focalLength": raw_exif.get("Canon:FocalLength") or raw_exif.get("EXIF:FocalLength") or raw_exif.get("MakerNotes:FocalLength") or "未知",
        "exposureBias": raw_exif.get("Canon:ExposureCompensation") or raw_exif.get("EXIF:ExposureBiasValue") or raw_exif.get("MakerNotes:ExposureCompensation") or "未知",
        "whiteBalance": raw_exif.get("Canon:WhiteBalance") or raw_exif.get("EXIF:WhiteBalance") or raw_exif.get("MakerNotes:WhiteBalance") or "未知"
    }

# ========== 上传接口 ==========
@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "未选择文件"}), 400
        
        file = request.files['file']
        original_filename = file.filename
        if original_filename == '' or not allowed_file(original_filename):
            return jsonify({"error": f"不支持的格式，支持: {', '.join(ALLOWED_EXTENSIONS)}"}), 400
        
        # 保存文件
        file_id = str(uuid.uuid4())
        ext = original_filename.rsplit('.', 1)[1].lower()
        filename = secure_filename(f"{file_id}.{ext}")
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        file.save(filepath)
        logging.info(f"上传文件: {filename} (原始名: {original_filename})")
        
        # 1. 获取完整原始EXIF（带截断）
        raw_exif = get_raw_exif(filepath)
        if not raw_exif:
            return jsonify({"error": "EXIF信息提取失败"}), 500
        
        # 2. 解析常用EXIF字段
        parsed_exif = parse_exif_for_display(raw_exif, original_filename)
        
        # 3. 自动识别所有预览图（优先JpgFromRaw）
        preview_tags, preview_meta = get_preview_tags(raw_exif, filepath)
        
        # 4. 构建预览图详情列表
        previews = []
        for tag in preview_tags[:5]:
            meta = preview_meta[tag]
            # 格式化大小（转换后大小）
            size_bytes = meta["converted_size"]
            if size_bytes < 1024:
                size_str = f"{size_bytes} B"
            elif size_bytes < 1024*1024:
                size_str = f"{size_bytes/1024:.2f} KB"
            else:
                size_str = f"{size_bytes/(1024*1024):.2f} MB"
            
            # 原始大小格式化
            raw_size_bytes = meta["size_bytes"]
            if raw_size_bytes < 1024:
                raw_size_str = f"{raw_size_bytes} B"
            elif raw_size_bytes < 1024*1024:
                raw_size_str = f"{raw_size_bytes/1024:.2f} KB"
            else:
                raw_size_str = f"{raw_size_bytes/(1024*1024):.2f} MB"
            
            previews.append({
                "tag": tag,
                "full_tag": meta["full_key"],
                "size_str": size_str,          # 转换后大小
                "size_bytes": size_bytes,
                "raw_size_str": raw_size_str,  # 原始大小
                "raw_size_bytes": raw_size_bytes,
                "width": meta["width"],
                "height": meta["height"],
                "resolution_str": f"{meta['width']}×{meta['height']}",
                "original_format": meta["original_format"],  # 原始格式
                "converted_format": meta["converted_format"],  # 转换后格式
                "used_tag": meta["used_tag"]  # 实际使用的标签
            })
        
        previews = [p for p in previews if p['size_bytes'] > 0]
        
        return jsonify({
            "code": 200,
            "msg": "上传成功",
            "data": {
                "file_id": file_id,
                "ext": ext,
                "raw_exif": raw_exif,
                "parsed_exif": parsed_exif,
                "previews": previews
            }
        })
        
    except Exception as e:
        logging.error(f"上传处理失败: {e}", exc_info=True)
        return jsonify({"code": 500, "error": f"处理失败: {str(e)}"}), 500

# ========== 提取转换后预览图接口（带EXIF） ==========
@app.route('/extract/<file_id>/<ext>/<tag>')
def extract_preview(file_id, ext, tag):
    """提取预览图并转换为Web兼容格式（带原始EXIF）"""
    try:
        filepath = os.path.join(UPLOAD_FOLDER, f"{file_id}.{ext}")
        if not os.path.exists(filepath):
            return jsonify({"code": 404, "error": "文件已过期或不存在"}), 404
        
        # 提取预览数据（优先JpgFromRaw）
        is_extract_success, binary_data, used_tag = extract_preview_data(filepath, tag)
        if not is_extract_success or not binary_data:
            return jsonify({"code": 500, "error": "预览图提取失败"}), 500
        
        # 转换为Web兼容格式，并复制EXIF
        is_success, converted_data, _, _, _ = convert_image_to_web_format(
            binary_data, 
            target_format='JPEG', 
            temp_file_prefix=filepath
        )
        
        if not is_success or not converted_data:
            return jsonify({"code": 500, "error": "预览图转换失败"}), 500
        
        # 流式返回转换后的数据
        def generate():
            yield converted_data
        
        # 安全处理文件名
        safe_tag = tag.replace('/', '_').replace(':', '_')
        download_name = f"{safe_tag}_{file_id[:8]}.jpg"
        
        return Response(
            generate(),
            mimetype='image/jpeg',
            headers={
                "Content-Disposition": f"inline; filename={download_name}",
                "Cache-Control": "no-cache, max-age=0",
                "Content-Length": str(len(converted_data))
            }
        )
    except Exception as e:
        logging.error(f"提取预览失败: {e}", exc_info=True)
        return jsonify({"code": 500, "error": "预览图提取失败"}), 500

# ========== 提取原始格式预览图接口（带EXIF）【优化】 ==========
@app.route('/extract_raw/<file_id>/<ext>/<tag>')
def extract_preview_raw(file_id, ext, tag):
    """提取原始格式预览图（如TIFF），并复制EXIF"""
    try:
        filepath = os.path.join(UPLOAD_FOLDER, f"{file_id}.{ext}")
        if not os.path.exists(filepath):
            return jsonify({"code": 404, "error": "文件已过期或不存在"}), 404
        
        # 提取原始预览数据
        is_extract_success, binary_data, used_tag = extract_preview_data(filepath, tag)
        if not is_extract_success or not binary_data:
            return jsonify({"code": 500, "error": "原始预览图提取失败"}), 500
        
        # 获取原始格式
        original_format = get_image_original_format(binary_data)
        format_ext = original_format.lower() if original_format != "UNKNOWN" else "bin"
        mime_type = f"image/{original_format.lower()}" if original_format in SUPPORTED_WEB_FORMATS else "application/octet-stream"
        
        # 生成唯一临时文件（避免冲突）
        temp_file = f"{filepath}_raw_preview_{uuid.uuid4().hex[:8]}.{format_ext}"
        with open(temp_file, 'wb') as f:
            f.write(binary_data)
        
        # 复制EXIF到原始预览文件
        copy_exif_to_preview(filepath, temp_file)
        
        # 读取带EXIF的原始数据
        with open(temp_file, 'rb') as f:
            raw_data_with_exif = f.read()
        
        # 清理临时文件
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
                logging.info(f"清理原始预览临时文件: {temp_file}")
            except Exception as e:
                logging.warning(f"清理原始预览临时文件失败: {e}")
        
        # 流式返回数据
        def generate():
            yield raw_data_with_exif
        
        # 安全处理文件名
        safe_tag = tag.replace('/', '_').replace(':', '_')
        download_name = f"{safe_tag}_{file_id[:8]}.{format_ext}"
        
        return Response(
            generate(),
            mimetype=mime_type,
            headers={
                "Content-Disposition": f"attachment; filename={download_name}",
                "Cache-Control": "no-cache, max-age=0",
                "Content-Length": str(len(raw_data_with_exif))
            }
        )
    except Exception as e:
        logging.error(f"提取原始预览失败: {e}", exc_info=True)
        return jsonify({"code": 500, "error": "原始预览图提取失败"}), 500

# ========== 错误处理 ==========
@app.errorhandler(RequestEntityTooLarge)
def handle_large_file(error):
    return jsonify({"code": 413, "error": "文件大小超过200MB限制"}), 413

@app.errorhandler(404)
def page_not_found(error):
    return jsonify({"code": 404, "error": "接口不存在"}), 404

# ========== 基础接口 ==========
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/exif/<file_id>/<ext>')
def get_exif(file_id, ext):
    """获取原始EXIF（调试用，带截断）"""
    try:
        filepath = os.path.join(UPLOAD_FOLDER, f"{file_id}.{ext}")
        if not os.path.exists(filepath):
            return jsonify({"code": 404, "error": "文件不存在"}), 404
        raw_exif = get_raw_exif(filepath)
        return jsonify({"code": 200, "data": raw_exif})
    except Exception as e:
        return jsonify({"code": 500, "error": str(e)}), 500

# ========== 程序入口 ==========
if __name__ == '__main__':
    try:
        app.run(host='0.0.0.0', port=10099, debug=False, threaded=True)
    finally:
        scheduler.shutdown()