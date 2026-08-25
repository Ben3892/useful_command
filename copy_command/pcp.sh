#!/usr/bin/env bash
# 用法: parallel_cp <SRC_DIR> <DST_DIR> [<MAX_JOBS>]
parallel_cp() {
    local src="$1"
    local dst="$2"
    local max_jobs="${3:-8}"

    # 检查参数
    if [[ -z "$src" || -z "$dst" ]]; then
        echo "Usage: parallel_cp <SRC_DIR> <DST_DIR> [<MAX_JOBS>]"
        return 1
    fi

    # 检查源目录是否存在
    if [[ ! -d "$src" ]]; then
        echo "Error: 源目录 $src 不存在！"
        return 1
    fi

    mkdir -p "$dst" || { echo "Error: 无法创建目标目录 $dst！"; return 1; }

    local jobs=0
    local tmp_list=$(mktemp)  # 用临时文件替代进程替换，兼容POSIX shell

    # 生成文件列表（null分隔，避免空格/特殊字符问题）
    find "$src" -type f -print0 > "$tmp_list"

    # 读取null分隔的文件列表
    while IFS= read -r -d '' file; do
        # 计算相对路径
        local rel="${file#$src/}"
        local target="$dst/$rel"
        local target_dir=$(dirname "$target")

        # 创建目标目录（忽略已存在的情况）
        mkdir -p "$target_dir" >/dev/null 2>&1

        # 后台复制文件
        cp -p "$file" "$target" &
        ((jobs++))

        # 控制并发数（兼容Bash <4.3的降级方案）
        if (( jobs >= max_jobs )); then
            # 方案1: 若Bash >=4.3，用wait -n（高效）；否则wait（等待所有）
            if [[ "${BASH_VERSINFO[0]}" -ge 4 && "${BASH_VERSINFO[1]}" -ge 3 ]]; then
                wait -n
            else
                wait
                jobs=0  # 重置计数
            fi
            ((jobs--))
        fi
    done < "$tmp_list"

    # 等待剩余后台任务完成
    wait
    # 清理临时文件
    rm -f "$tmp_list"

    echo "Parallel copy done! 源: $src -> 目标: $dst (并发数: $max_jobs)"
}

# 执行函数（传参）
parallel_cp "$1" "$2" "${3:-8}"