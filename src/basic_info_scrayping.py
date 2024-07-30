import requests
from bs4 import BeautifulSoup
import re
import os
import pandas as pd
import numpy as np
from tqdm import tqdm

# 指定されたURLからHTMLを取得し、BeautifulSoupオブジェクトを返す関数
def get_soup(url):
    """
    指定されたURLからHTMLコンテンツを取得し、BeautifulSoupオブジェクトを返す関数
    
    :param url: 取得するHTMLコンテンツのURL
    :return: BeautifulSoupオブジェクト
    """
    response = requests.get(url)
    response.encoding = 'utf-8'
    return BeautifulSoup(response.text, 'html.parser')

# HTMLのテキストをクリーニングし、余分なスペースを取り除く関数
def clean_text(soup):
    """
    BeautifulSoupオブジェクトからテキストを抽出し、余分なスペースを取り除く関数
    
    :param soup: BeautifulSoupオブジェクト
    :return: クリーニングされたテキスト
    """
    text_elements = soup.get_text()
    return re.sub(r'\s+', ' ', text_elements).strip()

# 特定のキーワードを基に情報を抽出する関数
def extract_info(words_list):
    """
    キーワード「リアルタイムに変更」と「比較される銘柄」の間の情報を抽出してリストにする関数
    
    :param words_list: クリーニングされたテキストを単語リストに変換したもの
    :return: 抽出した情報を含むリスト
    """
    realtime_index = words_list.index("リアルタイムに変更")
    hikaku_index = words_list.index("比較される銘柄")
    taishaku_info = words_list[realtime_index + 1 : hikaku_index]

    # 抽出する情報をリストとして返す
    first_four = taishaku_info[:3]
    index_of_gyouseki = taishaku_info.index('業績')
    gyousyu = taishaku_info[index_of_gyouseki + 1]
    last_six = taishaku_info[-6:]
    last_six.remove("時価総額")
    
    return first_four + [gyousyu] + last_six

# 抽出した情報を基にDataFrameを作成する関数
def create_taishaku_df(cleaned_taishaku_info):
    """
    抽出した基本情報を基にDataFrameを作成する関数
    
    :param cleaned_taishaku_info: 抽出した基本情報を含むリスト
    :return: 基本情報を含むDataFrame
    """
    columns = ['証券コード', '銘柄', '市場', '業種', 'PER', 'PBR', '利回り', '信用倍率', '時価総額']
    return pd.DataFrame([cleaned_taishaku_info], columns=columns)

# トレンド情報を抽出する関数
def extract_trend(words_list, gonitisen):
    """
    キーワード「5日線」などのトレンド情報を抽出する関数
    
    :param words_list: クリーニングされたテキストを単語リストに変換したもの
    :param gonitisen: 抽出対象のトレンド情報のキーワード
    :return: トレンド情報を含むリスト
    """
    if gonitisen in words_list:
        index_gonitisen = words_list.index(gonitisen)
        return words_list[index_gonitisen + 4 : index_gonitisen + 8]
    return []

# 推移情報を抽出する関数
def extract_suii(words_list, happyoubi, tansin):
    """
    推移情報を抽出し、「予」や「I」など、余計な文字を除外する関数
    
    :param words_list: クリーニングされたテキストを単語リストに変換したもの
    :param happyoubi: 推移情報の発表日キーワード
    :param tansin: 推移情報の決算短信キーワード
    :return: 推移情報を含むリスト
    """
    if happyoubi in words_list and tansin in words_list:
        index_happyoubi = words_list.index(happyoubi)
        index_tansin = words_list.index(tansin)
        if index_happyoubi < index_tansin:
            suii_list = words_list[index_happyoubi + 1 : index_tansin]
            return [x for x in suii_list if x not in ('予', 'I', '単', '連', '変')]
    return []

# トレンド情報を基にDataFrameを作成する関数
def create_trend_df(trend_list):
    """
    トレンド情報を基にDataFrameを作成する関数
    
    :param trend_list: トレンド情報を含むリスト
    :return: トレンド情報を含むDataFrame
    """
    return pd.DataFrame([trend_list], columns=['5日線', '25日線', '75日線', '200日線'])

# 推移情報を基にDataFrameを作成する関数
def create_suii_df(suii_list):
    """
    推移情報を基にDataFrameを作成する関数。データが7の倍数でない場合はNoneで埋める。
    
    :param suii_list: 推移情報を含むリスト
    :return: 推移情報を含むDataFrame
    """
    while len(suii_list) % 7 != 0:
        suii_list.append(None)
    suii_array = np.array(suii_list).reshape(-1, 7)
    return pd.DataFrame(suii_array, columns=['決算期', '売上高', '経常益', '最終益', '１株益', '１株配', '発表日'])

# 個別の証券コードに対して情報を取得し、DataFrameを作成する関数
def process_stock_code(i, kihon_df_list, suii_code_df_list):
    """
    証券コードに基づいて情報を取得し、基本情報と推移情報のDataFrameを作成する関数
    
    :param i: 証券コード
    :param kihon_df_list: 基本情報のDataFrameを追加するリスト
    :param suii_code_df_list: 推移情報のDataFrameを追加するリスト
    """
    url = f'https://kabutan.jp/stock/?code={str(i)}'
    soup = get_soup(url)
    clean_text_data = clean_text(soup)
    words_list = clean_text_data.split()

    # 「大株主」が存在しない場合はスキップ
    if "大株主" not in words_list:
        return
    
    # 基本情報を抽出してDataFrameを作成
    cleaned_taishaku_info = extract_info(words_list)
    taishaku_df = create_taishaku_df(cleaned_taishaku_info)

    # トレンド情報を抽出してDataFrameを作成
    trend_list = extract_trend(words_list, "5日線")
    suii_list = extract_suii(words_list, "発表日", "直近の決算短信")
    
    trend_df = create_trend_df(trend_list)
    suii_df = create_suii_df(suii_list)

    # 基本情報とトレンド情報を結合してリストに追加
    kihon_df = pd.concat([taishaku_df, trend_df], axis=1)
    kihon_df_list.append(kihon_df)

    # 推移情報と証券コードを結合してリストに追加
    shouken_code = pd.DataFrame({'証券コード': np.repeat(kihon_df['証券コード'].values, len(suii_df))})
    suii_code_df = pd.concat([shouken_code.reset_index(drop=True), suii_df.reset_index(drop=True)], axis=1)
    suii_code_df_list.append(suii_code_df)

# メイン関数
def main():
    """
    メイン関数。証券コード1300から9997までを処理し、結果をCSVファイルに保存する。
    """
    kihon_df_list = []
    suii_code_df_list = []

    # 証券コード1300から9997までを処理
    for i in tqdm(range(1300, 9998), desc="スクレイピング中"):
        try:
            process_stock_code(i, kihon_df_list, suii_code_df_list)
        except Exception as e:
            print(f"証券コード{i}には株価トレンド情報が無いので除外します: {e}")
            continue

    # データフレームを結合してCSVファイルとして保存
    SRC_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(SRC_DIR, '../data')
    all_kihon_df = pd.concat(kihon_df_list, ignore_index=True)
    all_suii_code_df = pd.concat(suii_code_df_list, ignore_index=True)
    all_kihon_df.to_csv(f'{DATA_DIR}/basic_info.csv', index=False, encoding='utf-8-sig')
    all_suii_code_df.to_csv(f'{DATA_DIR}/performance_trend.csv', index=False, encoding='utf-8-sig')

# メイン関数の呼び出し
if __name__ == "__main__":
    main()
