import time
from datetime import datetime

# 時間計測用のユーティリティ関数
def log_elapsed_time(message, start_time, include_timestamp=True):
    elapsed_time = time.time() - start_time
    timestamp_str = f"at {datetime.now()} " if include_timestamp else ""
    if isinstance(message, tuple) or isinstance(message, list):
        # 追加情報（カウントやサイズなど）がある場合
        main_message, *extra_info = message
        extra_str = ", ".join(str(info) for info in extra_info)
        print(f"{main_message}, {extra_str}, {timestamp_str}time elapsed: {elapsed_time:.2f} seconds")
    else:
        # 単純なメッセージの場合
        print(f"{message} {timestamp_str}time elapsed: {elapsed_time:.2f} seconds")
