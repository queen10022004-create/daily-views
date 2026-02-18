import os
import datetime
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

class YouTubeAnalyticsTool:
    def __init__(self, api_key):
        self.youtube = build('youtube', 'v3', developerKey=api_key)

    def get_channel_id_by_handle(self, handle):
        # ... (Giữ nguyên logic cũ) ...
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
        # ... (Giữ nguyên logic cũ) ...
        request = self.youtube.channels().list(part="contentDetails", id=channel_id)
        response = request.execute()
        try:
            return response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        except (IndexError, KeyError):
            return None

    def get_all_videos_stats(self, playlist_id):
        # ... (Giữ nguyên logic cũ, trả về danh sách dict) ...
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
                        'Likes_Current': int(stats.get('likeCount', 0)),
                        'Comments_Current': int(stats.get('commentCount', 0)),
                        'Views': int(stats.get('viewCount', 0)) # Sẽ được đổi tên ở hàm xử lý
                    })

            next_page_token = pl_response.get('nextPageToken')
            if not next_page_token:
                break
        return videos_data

    def update_history_csv(self, current_data, filename="youtube_history.csv"):
        """Xử lý Pandas: Trộn dữ liệu cũ và mới, tính toán lượt xem tăng thêm"""
        if not current_data:
            print("⚠️ Không có dữ liệu để xử lý.")
            return

        today_str = datetime.datetime.now().strftime('%Y-%m-%d')
        view_col_today = f'Views_{today_str}'

        # 1. Tạo DataFrame cho dữ liệu hôm nay
        df_today = pd.DataFrame(current_data)
        df_today.rename(columns={'Views': view_col_today}, inplace=True)

        # 2. Kiểm tra xem file lịch sử đã tồn tại chưa
        if os.path.exists(filename):
            print("📂 Tìm thấy dữ liệu cũ. Đang tiến hành so sánh...")
            df_hist = pd.read_csv(filename)
            
            # Lấy danh sách các cột Views cũ (bắt đầu bằng 'Views_' và chứa dấu '-')
            old_view_cols = [col for col in df_hist.columns if col.startswith('Views_') and '-' in col]
            
            # Lọc chỉ lấy Video ID và các cột Views cũ để merge (tránh trùng lặp thông tin Title, Link...)
            df_hist_views_only = df_hist[['Video ID'] + old_view_cols]
            
            # Gộp dữ liệu cũ và mới (Outer join để giữ cả video mới đăng hôm nay)
            df_final = pd.merge(df_today, df_hist_views_only, on='Video ID', how='outer')
            
            # Tính số view tăng thêm so với ngày gần nhất
            if old_view_cols:
                last_date_col = old_view_cols[-1]
                # Nếu video mới chưa có view cũ, coi view cũ là 0 để tính toán
                df_final['Views_Gained'] = df_final[view_col_today].fillna(0) - df_final[last_date_col].fillna(0)
            else:
                df_final['Views_Gained'] = 0
                
        else:
            print("🆕 Lần chạy đầu tiên! Đang tạo file lịch sử mới...")
            df_final = df_today
            df_final['Views_Gained'] = 0

        # Sắp xếp lại thứ tự cột cho đẹp mắt, lấp đầy các ô trống (NaN) bằng 0
        df_final = df_final.fillna(0)
        
        # Đẩy cột Views_Gained lên phía trước cho dễ nhìn
        cols = df_final.columns.tolist()
        cols.insert(4, cols.pop(cols.index('Views_Gained')))
        df_final = df_final[cols]

        # Sắp xếp theo view hôm nay giảm dần
        df_final = df_final.sort_values(by=view_col_today, ascending=False)
        
        # Lưu file
        df_final.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ Đã cập nhật thành công bảng thống kê vào file: {filename}")


# --- CẤU HÌNH CHẠY CHƯƠNG TRÌNH ---
if __name__ == "__main__":
    API_KEY = os.environ.get('YOUTUBE_API_KEY') 
    
    if not API_KEY:
        print("❌ LỖI: Chưa tìm thấy YOUTUBE_API_KEY trong biến môi trường.")
        exit(1)

    CHANNEL_HANDLE = '@GoogleDevelopers' # Thay bằng kênh của bạn
    CSV_FILENAME = f"history_{CHANNEL_HANDLE.replace('@','')}.csv"

    tool = YouTubeAnalyticsTool(API_KEY)
    channel_id = tool.get_channel_id_by_handle(CHANNEL_HANDLE)

    if channel_id:
        uploads_id = tool.get_uploads_playlist_id(channel_id)
        if uploads_id:
            data = tool.get_all_videos_stats(uploads_id)
            tool.update_history_csv(data, CSV_FILENAME)