#!/usr/bin/env python3

from loguru import logger
import subprocess
import time
import re

ansi_escape = re.compile(r'''
    \x1B  # ESC
    (?:   # 7-bit C1 Fe (except CSI)
        [@-Z\\-_]
    |     # or [ for CSI, followed by a control sequence
        \[
        [0-?]*  # Parameter bytes
        [ -/]*  # Intermediate bytes
        [@-~]   # Final byte
    )
''', re.VERBOSE)

logger.add("/storage/logs/py-scripts.log", rotation="50 MB", retention="30 days")
last_sectors = {}
pledge_paralle_cnt = 8
last_pledge_time = 0.0

def ansi_replace(text):
    return ansi_escape.sub('', text)


def parse_sectors_list(stdout):
    '''解析标准输出'''
    skip_header = True
    current_sectors = {}
    jobs_cnt = {}
    runing_sectors_cnt = 0

    for sector_info in stdout.split("\n"):
        if skip_header:
            skip_header = False
            continue
        if len(sector_info) == 0 or "RecoveryTimeout" in sector_info:
            continue
        try:
            splits = sector_info.strip().split()
            _id, state, on_chain, active = splits[:4]
            state = ansi_replace(state)
            on_chain = ansi_replace(on_chain)
            active = ansi_replace(active)
            # logger.debug("id={}, state={}, len(state)={}, onChain={}, len(onChain)={}", 
            #    _id, state, len(state), on_chain, len(on_chain))
            current_sectors[_id] = {
                "ID": _id, 
                "State": state,
                "OnChain": on_chain,
                "Active": active,
                # "Expiration": expiration,
                # "Deals": deals
            }
            if "Proving" in state or  "Removing" in state or _id in ('52'):
                continue
            runing_sectors_cnt = runing_sectors_cnt + 1
        except Exception as e:
            logger.error("解析失败, 不执行pledge, sector_info={}, length={}", sector_info, len(sector_info))
            return {}, pledge_paralle_cnt
    # logger.info("sectors={}", current_sectors)
    return current_sectors, runing_sectors_cnt


def compare_sectors_state(current_sectors):
    '''比较状态 生成变动消息'''
    cslen = len(current_sectors)
    lslen = len(last_sectors)
    if lslen == 0: 
        logger.info("第一次启动，不比较sectors状态")
        return
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
        running_cnt += 1
    logger.info("")


def check_sectors():
    '''运行 venus-sealer sectors list来检查状态'''
    global last_sectors
    global last_pledge_time
    process = subprocess.Popen(['venus-sealer', 'sectors', 'list', '--fast'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = process.communicate()
    current_sectors, running_cnt = parse_sectors_list(stdout)
    compare_sectors_state(current_sectors)
    if running_cnt < pledge_paralle_cnt: # 检查是否要运行 pledge
        logger.info("需要运行pledge, running={}, target={}", running_cnt, pledge_paralle_cnt)
        run_sectors_pledge(running_cnt)
        curr_time = time.time()
        logger.info("运行pledge需要{}s", curr_time - last_pledge_time)
        last_pledge_time = curr_time
    else:
        logger.info("不需要pledge, running={}", running_cnt)

    last_sectors = current_sectors


def main_loop():
    while True:
        logger.info("唤醒，开始检查sectors状态")
        check_sectors()
        logger.info("将会在60s后重新检查")
        time.sleep(60)



if __name__ == '__main__':
    logger.info("开始运行 auto_sector_pledge脚本")
    main_loop()
