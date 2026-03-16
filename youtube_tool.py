import os
import datetime
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

class YouTubeAnalyticsTool:
    def __init__(self, api_key):
        self.youtube = build('youtube', 'v3', developerKey=api_key)

    def get_channel_id_by_handle(self, handle):
        try:
            request = self.youtube.search().list(part="snippet", q=handle, type="channel", maxResults=1)
            response = request.execute()
            if response['items']:
                return response['items'][0]['snippet']['channelId']
            return None
        except HttpError as e:
            print(f"Lỗi khi tìm kênh: {e}")
            return None

    def get_uploads_playlist_id(self, channel_id):
        request = self.youtube.channels().list(part="contentDetails", id=channel_id)
        response = request.execute()
        try:
            return response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        except (IndexError, KeyError):
            return None

    def get_all_videos_stats(self, playlist_id):
        videos_data = []
        next_page_token = None
        print("🔄 Đang lấy dữ liệu từ YouTube API...")

        while True:
            pl_request = self.youtube.playlistItems().list(
                part="contentDetails", playlistId=playlist_id, maxResults=50, pageToken=next_page_token
            )
            pl_response = pl_request.execute()
            video_ids = [item['contentDetails']['videoId'] for item in pl_response['items']]

            if video_ids:
                vid_request = self.youtube.videos().list(part="snippet,statistics", id=','.join(video_ids))
                vid_response = vid_request.execute()

                for item in vid_response['items']:
                    stats = item['statistics']
                    snippet = item['snippet']
                    videos_data.append({
                        'Video ID': item['id'],
                        'Title': snippet['title'],
                        'Publish Date': snippet['publishedAt'],
                        'Link': f"https://www.youtube.com/watch?v={item['id']}",
                        'Views': int(stats.get('viewCount', 0))
                    })

            next_page_token = pl_response.get('nextPageToken')
            if not next_page_token:
                break
        return videos_data

    def update_history_excel(self, current_data, filename):
        if not current_data:
            print("⚠️ Không có dữ liệu.")
            return

        today_str = datetime.datetime.now().strftime('%Y-%m-%d')
        view_col_today = f'Views_{today_str}'

        # Tạo DataFrame cho hôm nay
        df_today = pd.DataFrame(current_data)
        df_today.rename(columns={'Views': view_col_today}, inplace=True)
        df_today[view_col_today] = pd.to_numeric(df_today[view_col_today], errors='coerce').fillna(0)

        df_final = df_today.copy()
        view_col_yesterday = None

        if os.path.exists(filename):
            print(f"📂 Đang lấy dữ liệu hôm qua từ file: {filename}")
            try:
                # Đọc Sheet 1 từ file Excel
                df_hist = pd.read_excel(filename, sheet_name=0)
                
                # Loại bỏ dòng "TOTAL" của lần chạy trước để không bị lỗi tính toán
                df_hist = df_hist[df_hist['Video ID'] != 'TOTAL']
                
                # Tìm cột chứa dữ liệu của ngày gần nhất (trừ ngày hôm nay ra nếu trùng)
                old_view_cols = [col for col in df_hist.columns if col.startswith('Views_') and '-' in col and col != view_col_today]
                old_view_cols.sort()
                
                if old_view_cols:
                    view_col_yesterday = old_view_cols[-1]
                    df_yesterday = df_hist[['Video ID', view_col_yesterday]].copy()
                    
                    # Chuyển đổi format chữ (ví dụ '1,234') thành số thực tế để làm toán
                    df_yesterday[view_col_yesterday] = df_yesterday[view_col_yesterday].astype(str).str.replace(',', '').astype(float)
                    
                    # Trộn dữ liệu
                    df_final = pd.merge(df_today, df_yesterday, on='Video ID', how='outer')
                    df_final['Views_Gained'] = df_final[view_col_today].fillna(0) - df_final[view_col_yesterday].fillna(0)
                else:
                    df_final['Views_Gained'] = 0
            except Exception as e:
                print(f"⚠️ Lỗi đọc file cũ: {e}. Hệ thống sẽ tạo lại từ đầu.")
                df_final['Views_Gained'] = 0
        else:
            print(f"🆕 Tạo file Excel mới: {filename}")
            df_final['Views_Gained'] = 0

        # Lấp đầy các ô trống bằng 0
        df_final = df_final.fillna(0)

        # CHỈ GIỮ LẠI: ID, Title, Publish, Link, Gained, Hôm qua, Hôm nay
        cols = ['Video ID', 'Title', 'Publish Date', 'Link', 'Views_Gained']
        if view_col_yesterday:
            cols.append(view_col_yesterday)
        cols.append(view_col_today)
        
        # Lọc các cột và sắp xếp
        df_final = df_final[cols].sort_values(by=view_col_today, ascending=False)

        # Sheet 2: Chỉ lấy những video có Views_Gained > 0
        df_changed = df_final[df_final['Views_Gained'] > 0].copy()

        # Hàm tiện ích: Thêm dòng Tổng Cộng và Format số hàng ngàn
        def format_and_add_total(df, num_cols):
            if df.empty:
                return df
                
            total_row = {col: '' for col in df.columns}
            total_row['Title'] = '🔥 TỔNG CỘNG'
            total_row['Video ID'] = 'TOTAL'
            
            # Tính tổng cho các cột số
            for col in num_cols:
                if col in df.columns:
                    total_row[col] = df[col].sum()
            
            # Gắn dòng tổng vào cuối bảng
            df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)
            
            # Format số hàng ngàn (thêm dấu phẩy)
            for col in num_cols:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: f"{int(x):,}" if pd.notnull(x) and x != '' else x)
            return df

        # Áp dụng format
        num_columns_to_format = ['Views_Gained', view_col_today]
        if view_col_yesterday:
            num_columns_to_format.append(view_col_yesterday)

        df_sheet1 = format_and_add_total(df_final.copy(), num_columns_to_format)
        df_sheet2 = format_and_add_total(df_changed.copy(), num_columns_to_format)

        # XUẤT RA EXCEL NHIỀU SHEET
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df_sheet1.to_excel(writer, sheet_name='Tat_ca_Video', index=False)
            df_sheet2.to_excel(writer, sheet_name='Video_Tang_View', index=False)
            
        print(f"✅ Đã lưu thành công file Excel (Tự động Format): {filename}")

if __name__ == "__main__":
    API_KEY = os.environ.get('API_KEY')
    if not API_KEY:
        API_KEY = os.environ.get('YOUTUBE_API_KEY') 

    if not API_KEY:
        print("❌ LỖI: Không tìm thấy API Key.")
        exit(1)

    CHANNEL_HANDLE = '@stoicether' 
    # QUAN TRỌNG: Đã đổi đuôi file thành .xlsx
    EXCEL_FILENAME = "history_stoicether.xlsx"

    tool = YouTubeAnalyticsTool(API_KEY)
    channel_id = tool.get_channel_id_by_handle(CHANNEL_HANDLE)

    if channel_id:
        uploads_id = tool.get_uploads_playlist_id(channel_id)
        if uploads_id:
            data = tool.get_all_videos_stats(uploads_id)
            tool.update_history_excel(data, EXCEL_FILENAME)
        else:
             print("❌ Không tìm thấy playlist Uploads.")
    else:
        print("❌ Không tìm thấy kênh.")
