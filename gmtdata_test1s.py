# 简化版快速开始
import asyncio
import datetime
import time
import sys
import re
import pandas as pd
from pathlib import Path
from playwright.async_api import async_playwright
import os
from dotenv import load_dotenv

def get_base_path():
    """获取程序的基础路径（支持打包成exe后的路径）"""
    if getattr(sys, 'frozen', False):
        # 打包成exe后运行
        return Path(sys.executable).parent
    else:
        # 正常Python脚本运行
        return Path(__file__).parent

# 查询列表示例（你可以从Excel读取）
QUERY_LIST = [
    {
        "时间段": "近一年",
        "海关编码": "",
        "产品名称": "car mat",
        "进口商": "",
        "出口商": ""
    },
    {
        "时间段": "2025-01-01~2025-03-31",
        "海关编码": "空白",
        "产品名称": "textile",
        "进口商": "空白",
        "出口商": "空白"
    }
]


async def extract_and_clean_table(page):
    """提取并清洗表格数据"""

    iframe = page.frame_locator('iframe[src*="tradePremium/search.aspx"]')

    # 等待表格
    await iframe.locator('#search-result').wait_for(timeout=10000)

    # 获取所有行
    rows = await iframe.locator('#search-result tbody tr').all()
    print(len(rows))
    temp_idx=0
    data = []
    for row in rows:
        # 获取所有单元格文本
        print(temp_idx,row)
        temp_idx += 1
        if temp_idx == 1:
            continue #cells = await row.locator('th').all()
        else :
            cells = await row.locator('td').all()
        row_dict = {}
        print(cells)

        # 日期
        date = await cells[0].text_content()
        row_dict['日期'] = date.strip() if date else ''

        # 海关编码
        hs_code = await cells[1].text_content()
        row_dict['海关编码res'] = re.sub(r'\s+', ' ', hs_code).strip() if hs_code else ''

        # 详细产品
        #product = await cells[2].text_content()
        product = await cells[2].locator('span').first.get_attribute('title')
        row_dict['详细产品'] = re.sub(r'\s+', ' ', product).strip() if product else ''

        # 进口商
        importer = await cells[3].get_attribute('title') #.text_content()
        row_dict['进口商res'] = re.sub(r'\s+', ' ', importer).strip() if importer else ''

        # 国外出口商
        exporter = await cells[4].get_attribute('title') #.text_content()
        row_dict['国外出口商res'] = re.sub(r'\s+', ' ', exporter).strip() if exporter else ''

        # 出口商所在国
        exporter_country = await cells[5].text_content()
        row_dict['出口商所在国'] = exporter_country.strip() if exporter_country else ''

        # 报关公司
        broker = await cells[6].text_content()
        row_dict['报关公司'] = broker.strip() if broker else ''

        # 净重（转换为数字）
        net_weight = await cells[7].text_content()
        try:
            row_dict['净重'] = float(net_weight.strip()) if net_weight and net_weight.strip() else 0
        except:
            row_dict['净重'] = 0

        # 美元金额（转换为数字）
        amount = await cells[8].text_content()
        try:
            row_dict['美元金额'] = float(amount.strip()) if amount and amount.strip() else 0
        except:
            row_dict['美元金额'] = 0

        data.append(row_dict)

    df = pd.DataFrame(data)

    # 数据类型转换
    # df['日期'] = pd.to_datetime(df['日期'])
    df['净重'] = df['净重'].astype(float)
    df['美元金额'] = df['美元金额'].astype(float)

    return df


async def scrape_all_pages(page):
    """采集所有页面的数据"""

    all_data = []
    page_num = 1
    iframe = page.frame_locator('iframe[src*="tradePremium/search.aspx"]')

    while True:
        print(f"正在采集第 {page_num} 页...")

        # 等待表格稳定加载
        await iframe.locator('#search-result').wait_for(timeout=10000)
        await page.wait_for_timeout(2000)  # 等待数据完全渲染

        # 提取当前页数据
        df = await extract_and_clean_table(page)
        all_data.append(df)

        # 检查是否有下一页
        next_btn = iframe.locator('a:has-text("下一页")')
        if await next_btn.count() == 0:
            break

        # 点击下一页
        await next_btn.click()
        await page.wait_for_load_state('networkidle')
        page_num += 1

    # 合并所有数据
    final_df = pd.concat(all_data, ignore_index=True)
    print(f"共采集 {len(final_df)} 条数据")

    return final_df



async def process_query(page, query):
    """处理单条查询"""
    ## 选择查询入口（国家）
    # 鼠标指向“海关交易数据”
    await page.hover('div.dsh_01:has-text("海关交易数据")')
    # 等待"美洲"模块展示
    await page.wait_for_selector('div:has-text("%s")' % query.get("地区", "全球"), state='visible')  # 根据文本
    fuceng = await page.wait_for_selector('div:has-text("%s")' % query.get("地区", "全球"), state='visible')
    button = await fuceng.query_selector('//a[contains(text(), "%s")]' % query.get("国家", ""))  # 在浮层内查找
    await button.click()
    print("country ok")

    # 先把iframe找出来
    # 通过src中的关键词定位到下拉框，点击下拉框，选择“近一年”
    iframe = page.frame_locator('iframe[src*="tradePremium/search.aspx"]')

    # 1、日期范围
    time_range = query.get("时间段", "")
    preset_options = ["最近月", "近一年", "全部", "本年", "上年", "前2年"]
    if time_range in preset_options:
        # 选择下拉框
        await iframe.locator('#daterange_chosen .chosen-single').click()
        await iframe.locator('#daterange_chosen .chosen-results li:has-text("%s")'%time_range).click()
    elif "~" in time_range:
        # 填写具体日期，输入严格按照【2025-01-01~2025-03-31】格式
        start, end = time_range.split("~")
        await iframe.locator('#cph113_searcher_txtbegindate').fill(start.strip()) # 开始日期
        await iframe.locator('#cph113_searcher_txtenddate').fill(end.strip()) # 结束日期
    print("time ok")

    # 2、勾选过滤项
    # 通过id定位
    await iframe.locator('#cph113_searcher_chknewcorp').check() #过滤货代物流企业
    await iframe.locator('#cph113_searcher_chknullcorp').check() #过滤空白企业
    print("✅ 已勾选checkbox")

    # 3、填入海关编码（如有）
    hscode = query.get("海关编码", "无")
    if hscode != '无':
        await iframe.locator('input[placeholder*="海关编码"]').fill(hscode)  # #_easyui_textbox_input12

    # 4、产品关键词（如有）
    product = query.get("产品关键词", "无")
    product_match = query.get("产品关键词匹配方式", "无")
    if product != "无":
        await iframe.locator('input[placeholder="请输入关键词"]').fill(product)  # #_easyui_textbox_input1
        # 使用下拉选择匹配方式
        if product_match not in ('无', '智能匹配'):
            # 点击下拉框，选择匹配方式，默认智能匹配
            await iframe.locator('#chkprecise_chosen .chosen-single').click()
            await iframe.locator('#chkprecise_chosen .chosen-results li:has-text("%s")'%product_match).click()
            print("product match ok")

    # 5、填入进口商（如有）
    importer = query.get("进口商", "无")
    importer_match = query.get("进口商匹配方式", "无")
    if importer != '无':
        await iframe.locator('input[placeholder="请输入进口商名称"]').fill(importer)  # #_easyui_textbox_input1
        # 使用下拉选择匹配方式
        if importer_match not in ('无', '完全匹配'):
            # 点击下拉框，选择匹配方式，默认完全匹配
            await iframe.locator('#chkcompprecise_chosen .chosen-single').click()
            await iframe.locator('#chkcompprecise_chosen .chosen-results li:has-text("%s")'%importer_match).click()
            print("importer match ok")

    # 6、填入出口商（如有）
    exporter = query.get("进口商", "无")
    exporter_match = query.get("进口商匹配方式", "无")
    if exporter != '无':
        await iframe.locator('input[placeholder="请输入出口商名称"]').fill(exporter)  # #_easyui_textbox_input1
        # 使用下拉选择匹配方式
        if exporter_match not in ('无', '完全匹配'):
            # 点击下拉框，选择匹配方式，默认完全匹配
            await iframe.locator('#chkexcompprecise_chosen .chosen-single').click()
            await iframe.locator('#chkexcompprecise_chosen .chosen-results li:has-text("%s")'%exporter_match).click()
            print("exporter match ok")

    # 点击“数据查询”按钮
    await iframe.locator('a#btnOk .enter-bt').click()
    # await iframe.locator('div:has-text("数据查询")').click()
    # await iframe.locator('#btnOk').click()

    # 重新获取iframe
    # iframe = page.frame_locator('iframe[src*="tradePremium/search.aspx"]')
    # 获取返回记录数量
    # 等待结果数量元素出现或更新（这个方法不行，因为本来就有这个元素在）
    # await iframe.locator('#ResultCount').wait_for(state='visible', timeout=30000)
    # print("查询结果已刷新")
    # 先等查完
    await page.wait_for_load_state('networkidle', timeout=30000)
    print("所有网络请求已完成")
    result_text = await iframe.locator('#ResultCount').text_content()
    result_count = int(result_text.strip())
    print(f"共 {result_count} 条结果")

    # 如果返回结果=0，则在查询条件后加上“查不到”
    if result_count == 0:
        result_df_thisquery = df = pd.DataFrame([['查不到'] * 9],
                  columns=['日期', '海关编码res', '详细产品', '进口商res',
                           '国外出口商res', '出口商所在国', '报关公司', '净重', '美元金额']) #【注意要和前面一致】
        temp_df_left = pd.DataFrame([query])
        temp_df_left['_key'] = 1
        result_df_thisquery['_key'] = 1
        all_df_thisquery = pd.merge(temp_df_left, result_df_thisquery, on='_key', how='left').drop('_key', axis=1)
        return all_df_thisquery

    # 如果返回结果>200，先看有没有导出按钮
    # 如果有导出按钮，则在查询条件后加上“数据可自行导出”（提示用户自己去网页下载）
    # 如果没有导出按钮，则进一步检查返回结果是否>400，是则在查询条件后加上“超过400条无法采集”
    # 如果进一步检查返回结果<=400，则这里不做任何事，直接等后面处理。
    if result_count > 200:
        export_btn = iframe.locator('a:has-text("导出数据")')
        if await export_btn.count() > 0:
            print("✅ 找到导出数据按钮")
            result_df_thisquery = df = pd.DataFrame([['数据可自行导出'] * 9],
                                                    columns=['日期', '海关编码res', '详细产品', '进口商res',
                                                             '国外出口商res', '出口商所在国', '报关公司', '净重',
                                                             '美元金额'])  # 【注意要和前面一致】
            temp_df_left = pd.DataFrame([query])
            temp_df_left['_key'] = 1
            result_df_thisquery['_key'] = 1
            all_df_thisquery = pd.merge(temp_df_left, result_df_thisquery, on='_key', how='left').drop('_key', axis=1)
            return all_df_thisquery
        elif result_count > 400:
            result_df_thisquery = df = pd.DataFrame([['超过400条无法采集'] * 9],
                                                    columns=['日期', '海关编码res', '详细产品', '进口商res',
                                                             '国外出口商res', '出口商所在国', '报关公司', '净重',
                                                             '美元金额'])  # 【注意要和前面一致】
            temp_df_left = pd.DataFrame([query])
            temp_df_left['_key'] = 1
            result_df_thisquery['_key'] = 1
            all_df_thisquery = pd.merge(temp_df_left, result_df_thisquery, on='_key', how='left').drop('_key', axis=1)
            return all_df_thisquery

    # # 单次使用
    # df = await extract_and_clean_table(page)
    # print(df.info())
    # # print(df.head())
    # print(df)

    # await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
    # await page.wait_for_timeout(1000)  # 等待滚动完成
    # await iframe.evaluate('window.scrollTo(0, document.body.scrollHeight)')
    # await page.wait_for_timeout(1000)

    # 测试点击下一页
    # next_btn = iframe.locator('a:has-text("下一页")')
    # print(next_btn)
    # # 等待下一页按钮出现（最多5秒）
    # await next_btn.wait_for(state='visible', timeout=5000)
    # await next_btn.click()
    # print("已点击下一页")

    # 保存到Excel
    result_df_thisquery = await scrape_all_pages(page)
    # print(result_df_thisquery)
    # result_df_thisquery.to_excel('query_results.xlsx', index=False)

    # 将输入和输出df拼接在一起形成all_df_thisquery
    temp_df_left = pd.DataFrame([query])
    # print(temp_df_left)
    temp_df_left['_key'] = 1
    result_df_thisquery['_key'] = 1

    # 执行 left join
    all_df_thisquery = pd.merge(temp_df_left, result_df_thisquery, on='_key', how='left').drop('_key', axis=1)
    # print(all_df_thisquery)
    # all_df_thisquery.to_excel('query_results.xlsx', index=False)

    return all_df_thisquery

CHROME_PATH = r"D:\python312\chrome-win\chrome.exe"
async def quick_scrape(query):
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            executable_path=CHROME_PATH,  # 指定浏览器可执行文件的完整路径
            headless=False)
        page = await browser.new_page()

        ## 1、访问并登录
        # 访问页面
        await page.goto('https://vip4.bigtradedata.com/')
        # 填入用户名和密码
        load_dotenv()
        usr_name = os.getenv('DB_USERNAME')
        temp_pw= os.getenv('DB_PASSWORD')
        # print(usr_name,temp_pw)
        selector_usr = 'input[name="username"]'
        selector_pw = 'input[type="password"]'
        if await page.is_visible(selector_usr, timeout=1000):
            await page.fill(selector_usr, usr_name)
        #if await page.is_visible(selector_pw, timeout=1000): 一般放在一起
            await page.fill(selector_pw, temp_pw)
        # 等待并点击
        await page.click('.login-check') #点击登录
        # 尝试处理弹窗，没有则跳过
        try:
            # 等待弹窗出现并点击确定按钮
            await page.wait_for_selector('.layui-layer-btn0:has-text("确定")', timeout=3000)
            await page.click('.layui-layer-btn0:has-text("确定")')
            print("已处理弹窗")
        except:
            print("没有弹窗，继续执行")

        # 输入查询条件
        all_df_thisquery = await process_query(page, query)
        await page.wait_for_timeout(2000)

        print("\n本次查询完成！")
        await browser.close()

        # time.sleep(600)
        return all_df_thisquery


def main():
# 获取程序基础路径（支持打包成exe）
    base_path = get_base_path()

    # 设置路径
    input_dir = base_path / 'input'
    output_filename='dataoutput'+datetime.datetime.now().strftime("%Y_%m_%d %H_%M_%S")+'.xlsx'
    output_file = base_path / 'output' / output_filename

    # 检查input文件夹是否存在
    if not input_dir.exists():
        print("错误: 找不到input文件夹，请创建input文件夹并放入Excel文件")
        print(f"当前程序路径: {base_path}")
        print("请在此路径下创建 'input' 文件夹")
        input("按回车键退出...")
        return

    file_path = input_dir / "gmt海关交易数据查询条件input20260423.xlsx"  # 【修改为你的文件名】
    df = pd.read_excel(file_path)

    # 确保列名与预期一致
    expected_columns = ['地区', '国家', '时间段', '海关编码','产品关键词', '产品关键词匹配方式'
                        ,'进口商', '进口商匹配方式','出口商', '出口商匹配方式']
    df = df[expected_columns]
    # print(df)

    # 循环处理每一行
    queryList = []
    for idx, row in df.iterrows():
        temp_query = row.to_dict()  # 每一行转换为字典
        # print(temp_query)
        queryList.append(temp_query)
    # print(queryList)  # 示例输出

    # 正式处理
    # 逐行处理
    final_df=pd.DataFrame() # 初始化最终表
    for i, query in enumerate(queryList):
        print(f"\n处理第 {i + 1} 条查询...")
        temp_all_df = asyncio.run(quick_scrape(query))
        final_df = pd.concat([final_df, temp_all_df], ignore_index=True)
    print("\n全部查询完成！")
    # print(final_df)

    # 最终结果输出
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        final_df.to_excel(writer, sheet_name='Sheet1', index=False)
        print(f"✓ 明细数据已保存，共 {len(final_df)} 行")

if __name__ == "__main__":
    main()