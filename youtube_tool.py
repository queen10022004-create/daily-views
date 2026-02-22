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

    def update_history_csv(self, current_data, filename):
        if not current_data:
            print("⚠️ Không có dữ liệu.")
            return

        # 1. Xác định tên cột cho ngày hôm nay
        today_str = datetime.datetime.now().strftime('%Y-%m-%d')
        view_col_today = f'Views_{today_str}'

        # Tạo bảng dữ liệu mới
        df_today = pd.DataFrame(current_data)
        df_today.rename(columns={'Views': view_col_today}, inplace=True)

        if os.path.exists(filename):
            print(f"📂 Đang cập nhật vào file: {filename}")
            df_hist = pd.read_csv(filename)
            
            # Xóa cột dữ liệu hôm nay nếu đã tồn tại (để tránh trùng lặp khi chạy lại)
            if view_col_today in df_hist.columns:
                df_hist.drop(columns=[view_col_today], inplace=True)

            # Lấy danh sách các cột Views cũ
            old_view_cols = [col for col in df_hist.columns if col.startswith('Views_') and '-' in col]
            
            # QUAN TRỌNG: Sắp xếp các cột theo ngày tăng dần để tìm đúng ngày liền kề
            old_view_cols.sort()

            # Chỉ giữ lại Video ID và các cột Views lịch sử để ghép nối
            df_hist_views_only = df_hist[['Video ID'] + old_view_cols]
            df_final = pd.merge(df_today, df_hist_views_only, on='Video ID', how='outer')
            
            # Tính toán Views_Gained (Hôm nay - Ngày gần nhất)
            if old_view_cols:
                last_date_col = old_view_cols[-1] # Lấy cột ngày mới nhất trong quá khứ
                print(f"📊 Đang tính chênh lệch giữa {view_col_today} và {last_date_col}")
                
                # Tính chênh lệch, nếu dữ liệu thiếu thì coi là 0
                df_final['Views_Gained'] = df_final[view_col_today].fillna(0) - df_final[last_date_col].fillna(0)
            else:
                df_final['Views_Gained'] = 0
        else:
            print(f"🆕 Tạo file mới: {filename}")
            df_final = df_today
            df_final['Views_Gained'] = 0

        # Xử lý hiển thị: Điền 0 vào ô trống
        df_final = df_final.fillna(0)
        
        # Đưa cột Views_Gained lên vị trí thứ 5 (sau Link) cho dễ nhìn
        cols = df_final.columns.tolist()
        if 'Views_Gained' in cols:
            cols.insert(4, cols.pop(cols.index('Views_Gained')))
        
        # Sắp xếp danh sách theo lượt view hôm nay giảm dần
        df_final = df_final[cols].sort_values(by=view_col_today, ascending=False)
        
        # Lưu file
        df_final.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ Đã lưu thành công file: {filename}")

if __name__ == "__main__":
    # Lấy API Key
    API_KEY = os.environ.get('API_KEY')
    if not API_KEY:
        API_KEY = os.environ.get('YOUTUBE_API_KEY') 

    if not API_KEY:
        print("❌ LỖI: Không tìm thấy API Key.")
        exit(1)

    # --- CẬP NHẬT THÔNG TIN CỦA BẠN ---
    CHANNEL_HANDLE = '@stoicether' 
    CSV_FILENAME = "history_stoicether.csv"
    # ----------------------------------

    tool = YouTubeAnalyticsTool(API_KEY)
    channel_id = tool.get_channel_id_by_handle(CHANNEL_HANDLE)

    if channel_id:
        uploads_id = tool.get_uploads_playlist_id(channel_id)
        if uploads_id:
            data = tool.get_all_videos_stats(uploads_id)
            tool.update_history_csv(data, CSV_FILENAME)
        else:
             print("❌ Không tìm thấy playlist Uploads.")
    else:
        print("❌ Không tìm thấy kênh.")
