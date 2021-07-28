#!/usr/bin/env python3

from loguru import logger
import subprocess
import time
import re

ansi_escape = re.compile(r'''
    \x1B  # ESC
    (?:   # 7-bit C1 Fe (except CSI)
        [@-Z\\-_]
    |     
        \[
        [0-?]*  # Parameter bytes
        [ -/]*  # Intermediate bytes
        [@-~]   # Final byte
    )
''', re.VERBOSE)

logger.add("/root/logs/py-scripts.log", rotation="50 MB", retention="30 days")
last_sectors = {}
pledge_paralle_cnt = 3

def ansi_replace(text):
    return ansi_escape.sub('', text)


def parse_sectors_list(stdout):
    '''解析标准输出'''
    skip_header = True
    current_sectors = {}
    runing_sectors_cnt = 0

    for sector_info in stdout.split("\n"):
        if skip_header:
            skip_header = False
            continue
        if len(sector_info) == 0:
            continue
        try:
            splits = sector_info.strip().split()
            _id, state, on_chain, active = splits[:4]
            state = ansi_replace(state)
            on_chain = ansi_replace(on_chain)
            active = ansi_replace(active)
            current_sectors[_id] = {
                "ID": _id, 
                "State": state,
                "OnChain": on_chain,
                "Active": active,
                # "Expiration": expiration,
                # "Deals": deals
            }
            if state in ["Proving", "Removing"]:
                continue
            runing_sectors_cnt = runing_sectors_cnt + 1
        except Exception as e:
            logger.error("解析失败, 不执行pledge, sector_info={}, length={}", sector_info, len(sector_info))
            return {}, pledge_paralle_cnt
    logger.info("sectors={}", current_sectors)
    return current_sectors, runing_sectors_cnt


def compare_sectors_state(current_sectors):
    '''比较状态 生成变动消息'''
    cslen = len(current_sectors)
    lslen = len(last_sectors)
    if cslen > lslen:
        logger.info("发现sectors个数变动, current:{} - {}", cslen, lslen)
    # 对比当前与现在的
    for key in current_sectors.keys():
        current_sector_state = current_sectors[key]
        if key in last_sectors:
            last_sector_state = last_sectors[key]
        else:
            css = current_sector_state['State']
            logger.info("发现sector新增, sector id = {}, state = {}", key, css)
            continue
        if current_sector_state['State'] != last_sector_state['State']:
            css = current_sector_state['State']
            lss =  last_sector_state['State']
            logger.info("发现sector变动, sector id={}, statue {} -> {}", key, lss, css)

    
def run_sectors_pledge(running_cnt):
    i = running_cnt
    while running_cnt < pledge_paralle_cnt:
        process = subprocess.Popen(['venus-sealer', 'sectors', 'pledge'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        stdout, stderr = process.communicate()
        logger.info("运行 pledege完成, stdout={}", stdout)


def check_sectors():
    '''运行 venus-sealer sectors list来检查状态'''
    global last_sectors
    process = subprocess.Popen(['venus-sealer', 'sectors', 'list'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = process.communicate()
    current_sectors, running_cnt = parse_sectors_list(stdout)
    last_sectors = current_sectors
    if running_cnt < pledge_paralle_cnt: # 检查是否要运行 pledge
        logger.info("需要运行pledge, running={}, target={}", running_cnt, pledge_paralle_cnt)
        run_sectors_pledge(running_cnt)
    else:
        logger.info("不需要pledge, running={}", running_cnt)


def main_loop():
    while True:
        logger.info("唤醒，开始检查sectors状态")
        check_sectors()
        time.sleep(120)



if __name__ == '__main__':
    logger.info("开始运行 auto_sector_pledge脚本")
    main_loop()
