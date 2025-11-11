#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scan_tree.py — сканер директории -> JSON-дерево с содержимым файлов.

Добавлена опция --shallow-dir для указания директорий, в которых нужно
показывать только имена файлов/папок без чтения содержимого и без хешей.
Добавлены опции для разбиения выходного JSON на части (--split-size) и
минификации (--minify).
Пример:
    python scan_tree.py . --out tree.json --shallow-dir artifacts,dist --split-size 409600
"""

from __future__ import annotations
import os
import sys
import json
import base64
import hashlib
import argparse
import fnmatch
from typing import Dict, Any, List, Optional

# ----------------- существующие функции (не трогал) ----------------------

def parse_patterns(csv: str | None) -> List[str]:
    if not csv:
        return []
    # Убираем пустые элементы и пробелы
    return [p.strip() for p in csv.split(",") if p.strip()]


def match_any(name: str, patterns: List[str]) -> bool:
    return any(fnmatch.fnmatch(name, pat) for pat in patterns)


def is_hidden(name: str) -> bool:
    # UNIX: скрытые начинаются с точки; на Windows используем это же правило
    return name.startswith(".")


def file_entry(
    full_path: str,
    rel_path: str,
    max_file_bytes: int,
    include_hash: bool,
) -> Dict[str, Any]:
    entry: Dict[str, Any] = {
        "type": "file",
        "name": os.path.basename(rel_path),
        "path": rel_path.replace("\\", "/"),
    }

    try:
        size = os.path.getsize(full_path)
        entry["size"] = int(size)
    except Exception as e:
        entry["error"] = f"stat_failed: {e}"
        return entry

    # Считываем не больше max_file_bytes (+1 для флага truncated)
    try:
        with open(full_path, "rb") as f:
            data = f.read(max_file_bytes + 1)
    except Exception as e:
        entry["error"] = f"read_failed: {e}"
        return entry

    truncated = len(data) > max_file_bytes
    if truncated:
        data = data[:max_file_bytes]
        entry["truncated"] = True

    # Пробуем как utf-8
    is_text = False
    try:
        text = data.decode("utf-8")  # строго
        is_text = True
    except UnicodeDecodeError:
        is_text = False

    if is_text:
        entry["encoding"] = "utf-8"
        entry["content"] = text
    else:
        entry["binary"] = True
        entry["content_base64"] = base64.b64encode(data).decode("ascii")

    if include_hash:
        try:
            sha = hashlib.sha256()
            with open(full_path, "rb") as fh:
                for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                    sha.update(chunk)
            entry["sha256"] = sha.hexdigest()
        except Exception as e:
            entry["hash_error"] = f"{e}"

    return entry


def dir_entry(
    full_path: str,
    rel_path: str,
    exclude_dirs: List[str],
    exclude_files: List[str],
    max_file_bytes: int,
    include_hash: bool,
    follow_symlinks: bool,
    skip_hidden: bool,
    shallow_dirs: List[str],
) -> Dict[str, Any]:
    entry: Dict[str, Any] = {
        "type": "directory",
        "name": os.path.basename(rel_path) if rel_path else os.path.basename(os.path.abspath(full_path)),
        "path": (rel_path or ".").replace("\\", "/"),
        "children": [],
    }

    try:
        with os.scandir(full_path) as it:
            for de in it:
                name = de.name
                child_full = os.path.join(full_path, name)
                child_rel = os.path.join(rel_path, name) if rel_path else name

                # Скрытые?
                if skip_hidden and is_hidden(name):
                    continue

                # Симлинки
                try:
                    is_link = de.is_symlink()
                except Exception:
                    is_link = os.path.islink(child_full)

                if is_link and not follow_symlinks:
                    entry["children"].append({
                        "type": "symlink",
                        "name": name,
                        "path": child_rel.replace("\\", "/"),
                        "target": os.readlink(child_full) if os.path.islink(child_full) else None
                    })
                    continue

                # Директория?
                try:
                    is_dir = de.is_dir(follow_symlinks=follow_symlinks)
                except Exception:
                    is_dir = os.path.isdir(child_full)

                # Файл?
                try:
                    is_file = de.is_file(follow_symlinks=follow_symlinks)
                except Exception:
                    is_file = os.path.isfile(child_full)

                if is_dir:
                    if match_any(name, exclude_dirs):
                        continue

                    # Если это одна из shallow-директорий — не углубляемся,
                    # а просто даём список имён (без чтения/хешей).
                    if match_any(name, shallow_dirs):
                        try:
                            items = os.listdir(child_full)
                        except Exception as e:
                            entry["children"].append({
                                "type": "directory_summary",
                                "name": name,
                                "path": child_rel.replace("\\", "/"),
                                "error": f"list_failed: {e}"
                            })
                            continue

                        children_summary = []
                        for it_name in sorted(items):
                            it_full = os.path.join(child_full, it_name)
                            try:
                                if os.path.isdir(it_full):
                                    t = "directory"
                                elif os.path.isfile(it_full):
                                    t = "file"
                                else:
                                    t = "other"
                            except Exception:
                                t = "other"
                            children_summary.append({
                                "type": t,
                                "name": it_name,
                                "path": os.path.join(child_rel, it_name).replace("\\", "/")
                            })

                        entry["children"].append({
                            "type": "directory_summary",
                            "name": name,
                            "path": child_rel.replace("\\", "/"),
                            "children_count": len(children_summary),
                            "children": children_summary,
                        })
                        continue

                    # Обычная рекурсия
                    entry["children"].append(
                        dir_entry(
                            child_full,
                            child_rel,
                            exclude_dirs,
                            exclude_files,
                            max_file_bytes,
                            include_hash,
                            follow_symlinks,
                            skip_hidden,
                            shallow_dirs,
                        )
                    )
                elif is_file:
                    if match_any(name, exclude_files):
                        continue
                    entry["children"].append(
                        file_entry(
                            child_full,
                            child_rel,
                            max_file_bytes,
                            include_hash,
                        )
                    )
                else:
                    entry["children"].append({
                        "type": "other",
                        "name": name,
                        "path": child_rel.replace("\\", "/"),
                    })

    except Exception as e:
        entry["error"] = f"list_failed: {e}"

    return entry


def build_tree(
    root: str,
    exclude_dirs: List[str],
    exclude_files: List[str],
    max_file_bytes: int,
    include_hash: bool,
    follow_symlinks: bool,
    skip_hidden: bool,
    shallow_dirs: List[str],
) -> Dict[str, Any]:
    root_abs = os.path.abspath(root)
    if not os.path.exists(root_abs):
        raise FileNotFoundError(f"Path not found: {root_abs}")
    if os.path.isfile(root_abs):
        # Если передан файл, вернём только его
        return file_entry(root_abs, os.path.basename(root_abs), max_file_bytes, include_hash)
    # Директория
    tree = dir_entry(
        full_path=root_abs,
        rel_path="",
        exclude_dirs=exclude_dirs,
        exclude_files=exclude_files,
        max_file_bytes=max_file_bytes,
        include_hash=include_hash,
        follow_symlinks=follow_symlinks,
        skip_hidden=skip_hidden,
        shallow_dirs=shallow_dirs,
    )
    tree["meta"] = {
        "root": root_abs.replace("\\", "/"),
        "max_file_bytes": max_file_bytes,
        "follow_symlinks": follow_symlinks,
        "skip_hidden": skip_hidden,
        "exclude_dirs": exclude_dirs,
        "exclude_files": exclude_files,
        "hash": include_hash,
        "shallow_dirs": shallow_dirs,
    }
    return tree

# ----------------- новые вспомогательные функции для сплита -----------------

def write_bytes_in_parts(base_out: str, data: bytes, part_size: int) -> List[str]:
    """
    Разбивает байты data на части по part_size и пишет файлы:
      <base_out>.part001.json, <base_out>.part002.json, ...
    Возвращает список созданных файлов (в порядке).
    """
    os.makedirs(os.path.dirname(base_out) or ".", exist_ok=True)
    base_dir, base_name = os.path.split(base_out)
    name_root, ext = os.path.splitext(base_name)
    if not ext:
        ext = ".json"
    files = []
    total = len(data)
    parts = (total + part_size - 1) // part_size
    for i in range(parts):
        start = i * part_size
        chunk = data[start:start + part_size]
        part_filename = os.path.join(base_dir, f"{name_root}.part{i+1:03d}{ext}")
        with open(part_filename, "wb") as pf:
            pf.write(chunk)
        files.append(part_filename)
    return files


def save_index_file(base_out: str, parts: List[str], original_size: int) -> None:
    base_dir, base_name = os.path.split(base_out)
    name_root, ext = os.path.splitext(base_name)
    index_filename = os.path.join(base_dir, f"{name_root}.parts.json")
    index = {
        "original_name": base_name,
        "original_size": original_size,
        "parts": [os.path.basename(p) for p in parts],
    }
    with open(index_filename, "w", encoding="utf-8") as idxf:
        json.dump(index, idxf, ensure_ascii=False, indent=2)


def assemble_parts_cli(index_path: str, out_path: Optional[str] = None) -> None:
    """
    Утилита (встроенная) для склейки частей по index-файлу.
    Использование: python scan_tree.py --assemble parts.index.json
    """
    with open(index_path, "r", encoding="utf-8") as f:
        info = json.load(f)
    base_dir = os.path.dirname(index_path)
    parts = info.get("parts", [])
    if not parts:
        raise ValueError("Index file содержит 0 частей.")
    out_name = out_path or info.get("original_name", "assembled.json")
    out_full = os.path.join(base_dir, out_name)
    with open(out_full, "wb") as out_f:
        for p in parts:
            part_path = os.path.join(base_dir, p)
            with open(part_path, "rb") as pf:
                out_f.write(pf.read())
    print(f"OK: собран {out_full}")

# ----------------- main -----------------

def main():
    ap = argparse.ArgumentParser(description="Сканирует директорию и делает JSON-дерево с содержимым файлов.")
    ap.add_argument("path", nargs="?", help="Корневая директория (или файл) для сканирования, например '.'")
    ap.add_argument("--out", "-o", help="Путь к выходному JSON (по умолчанию stdout)")
    ap.add_argument("--max-file-bytes", type=int, default=256 * 1024,
                    help="Сколько максимум байт читать из каждого файла (по умолчанию 262144)")
    ap.add_argument("--exclude-dir", default=".git,node_modules,__pycache__,.venv,venv,dist,build",
                    help="CSV-маски директорий для исключения (fnmatch). Пример: '.git,node_modules'")
    ap.add_argument("--exclude-file", default="",
                    help="CSV-маски файлов для исключения (fnmatch). Пример: '*.pyc,*.o,*.log'")
    ap.add_argument("--hash", action="store_true", help="Добавлять SHA-256 файлов (дорого по времени).")
    ap.add_argument("--follow-symlinks", action="store_true", help="Следовать по симлинкам директорий/файлов.")
    ap.add_argument("--include-hidden", action="store_true", help="Включать скрытые файлы/папки (.*).")
    ap.add_argument("--shallow-dir", default="artifacts",
                    help="CSV-маски директорий, которые показывать 'мелко' (только имена файлов/папок), "
                         "например 'artifacts,dist'. По умолчанию 'artifacts'.")
    ap.add_argument("--split-size", type=int, default=400 * 1024,
                    help="Максимальный размер части в байтах. Если выходной JSON больше - будет разбит.")
    ap.add_argument("--minify", action="store_true",
                    help="Минифицировать JSON перед записью (меньше пробелов/переносов).")
    ap.add_argument("--assemble", help="Собрать части по index-файлу (пример: tree.parts.json).")
    args = ap.parse_args()

    if args.assemble:
        assemble_parts_cli(args.assemble, None)
        return

    if not args.path:
        ap.error("Требуется указать path (например '.'). Используй --assemble для склейки частей.")

    exclude_dirs = parse_patterns(args.exclude_dir)
    exclude_files = parse_patterns(args.exclude_file)
    skip_hidden = not args.include_hidden
    shallow_dirs = parse_patterns(args.shallow_dir)

    tree = build_tree(
        root=args.path,
        exclude_dirs=exclude_dirs,
        exclude_files=exclude_files,
        max_file_bytes=args.max_file_bytes,
        include_hash=args.hash,
        follow_symlinks=args.follow_symlinks,
        skip_hidden=skip_hidden,
        shallow_dirs=shallow_dirs,
    )

    # Сериализация: либо минифицированная, либо "красивый" вывод
    if args.minify:
        json_bytes = json.dumps(tree, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    else:
        json_bytes = json.dumps(tree, ensure_ascii=False, indent=2).encode("utf-8")

    # Если out не указан — печатаем в stdout (не делаем split)
    if not args.out:
        # Печатаем как текст в stdout
        try:
            sys.stdout.buffer.write(json_bytes)
            sys.stdout.buffer.write(b"\n")
        except Exception:
            # fallback для старых сред
            print(json_bytes.decode("utf-8"))
        return

    # Если указан out — проверяем размер и записываем либо одним файлом, либо частями
    out_path = args.out
    part_size = max(1, args.split_size)

    # Создаём директорию если нужно
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    if len(json_bytes) <= part_size:
        # простая запись
        with open(out_path, "wb") as f:
            f.write(json_bytes)
        print(f"OK: JSON сохранён в {out_path} ({len(json_bytes)} bytes)")
    else:
        # разбиваем на части
        parts = write_bytes_in_parts(out_path, json_bytes, part_size)
        save_index_file(out_path, parts, len(json_bytes))
        print(f"OK: JSON ({len(json_bytes)} bytes) разбит на {len(parts)} частей. Файлы:")
        for p in parts:
            print("  -", p)
        base_dir, base_name = os.path.split(out_path)
        name_root, ext = os.path.splitext(base_name)
        idxname = os.path.join(base_dir, f"{name_root}.parts.json")
        print("Индексная инфа сохранена в:", idxname)
        print()
        print("Склейка (unix):")
        print(f"  cat {' '.join([os.path.basename(p) for p in parts])} > {os.path.basename(out_path)}")
        print("Или (python):")
        print(f"  python scan_tree.py --assemble {os.path.basename(idxname)}")

if __name__ == "__main__":
    main()
