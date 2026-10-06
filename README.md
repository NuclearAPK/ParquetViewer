# Parquet Viewer

Веб-приложение для просмотра Parquet-файлов и Parquet-наборов данных (папок) со схемами — включая Iceberg-схемы.

## Возможности

- **Открытие по пути** — папка-набор данных (`*.parquet`, `iceberg.schema.json`, `_common_metadata`) или одиночный `.parquet` файл
- **Загрузка файлов** — drag & drop или выбор файла через интерфейс
- **Просмотр схемы** — таблица полей: ID, имя, тип Iceberg, тип Arrow/Parquet, NotNull, описание поля + комментарий таблицы (из `iceberg.schema.json`)
- **Просмотр данных** — таблица с липкими заголовками, пагинация, выбор файла в наборе, типы колонок
- **Поиск** — построчный фильтр по строковым колонкам (работает и по UUID-хексу), с дебаунсом
- **Экспорт** — CSV и JSON
- **Обзор ФС сервера** — модальное окно навигации по папкам для поиска наборов
- **Сериализация сложных типов**:
  - UUID (`bytes[16]`) → `97e76448-c0df-11f1-8d49-f83dc61a403d`
  - Timestamp → ISO 8601
  - Decimal → число

## Стек

- **Backend:** Python 3.13, FastAPI, PyArrow, pandas
- **Frontend:** vanilla HTML/CSS/JS (раздаётся FastAPI на `/static`)

## Установка и запуск

```bash
pip install -r requirements.txt
python -m uvicorn main:app --host 127.0.0.1 --port 8006
```

Открыть: http://127.0.0.1:8006/static/index.html

При загрузке страницы автоматически открывается путь из поля ввода (по умолчанию — последний используемый набор).

## Пример данных

Сгенерировать тестовый файл (50 строк, 5 колонок):

```bash
python create_sample.py   # создаёт sample_data.parquet
```

## API

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/health` | Статус сервера и число открытых наборов |
| `GET` | `/fs?path=` | Обзор директории (папки, `.parquet`-файлы, наличие схемы) |
| `POST` | `/open` | Открыть набор: `{"path": "C:\\...\\dataset"}` |
| `POST` | `/upload` | Загрузить `.parquet` файл (multipart) |
| `GET` | `/ds` | Список открытых наборов |
| `GET` | `/ds/{id}` | Схема и метаданные набора |
| `GET` | `/ds/{id}/data` | Строки: `file`, `limit`, `offset`, `search` |
| `GET` | `/ds/{id}/export` | Экспорт: `format=csv\|json`, `file` |

## Структура проекта

```
ParquetViewer/
├── main.py              # FastAPI backend: открытие наборов, схема, данные, экспорт
├── app/
│   └── static/
│       └── index.html   # Frontend: загрузка, обзор ФС, вкладки «Схема» / «Данные»
├── create_sample.py     # Генерация sample_data.parquet
├── requirements.txt
└── uploads/             # Загруженные файлы (в .gitignore)
```

## Формат набора данных

Приложение рассчитано на папки вида (пример — Iceberg-экстракция):

```
dataset/
├── part-000001.parquet   # данные
├── iceberg.schema.json   # схема Iceberg: поля, типы, описания, комментарий таблицы
└── _common_metadata      # метаданные схемы Parquet/Arrow
```

`iceberg.schema.json` читается как UTF-8 (fallback — cp1251).
