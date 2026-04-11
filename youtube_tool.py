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
            print(f"Lỗi: {e}")
            return None

    def get_uploads_playlist_id(self, channel_id):
        request = self.youtube.channels().list(part="contentDetails", id=channel_id)
        response = request.execute()
        try:
            return response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        except: return None

    def get_all_videos_stats(self, playlist_id):
        videos_data = []
        next_page_token = None
        print("🔄 Đang lấy dữ liệu mới từ YouTube...")
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
                        'Publish Date': snippet['publishedAt'][:10], # Lấy yyyy-mm-dd
                        'Link': f"https://www.youtube.com/watch?v={item['id']}",
                        'Views': int(stats.get('viewCount', 0))
                    })
            next_page_token = pl_response.get('nextPageToken')
            if not next_page_token: break
        return videos_data

    def generate_strict_report(self, current_data, hist_file, report_file):
        today_str = datetime.datetime.now().strftime('%Y-%m-%d')
        yesterday_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        
        col_today = f'Views_{today_str}'
        col_yesterday = f'Views_{yesterday_str}'

        df_today = pd.DataFrame(current_data)
        df_today.rename(columns={'Views': col_today}, inplace=True)

        # 1. Quản lý tệp lịch sử (Để lưu trữ lâu dài)
        if os.path.exists(hist_file):
            df_hist = pd.read_excel(hist_file)
            # Loại bỏ dòng tổng cũ
            df_hist = df_hist[df_hist['Video ID'] != 'TOTAL']
            # Cập nhật hoặc thêm ngày mới
            if col_today in df_hist.columns: df_hist.drop(columns=[col_today], inplace=True)
            df_combined = pd.merge(df_today, df_hist[['Video ID'] + [c for c in df_hist.columns if 'Views_' in c]], on='Video ID', how='left')
        else:
            df_combined = df_today.copy()

        # Lưu lại file lịch sử gốc (giữ tối đa 10 cột để làm kho dữ liệu)
        view_cols = sorted([c for c in df_combined.columns if 'Views_' in c])
        if len(view_cols) > 10: df_combined.drop(columns=view_cols[:-10], inplace=True)
        df_combined.to_excel(hist_file, index=False)

        # 2. Tạo báo cáo gửi Email (Chỉ gồm 2 ngày liền kề)
        # Kiểm tra xem có dữ liệu ngày hôm qua không
        if col_yesterday in df_combined.columns:
            prev_data_col = col_yesterday
        else:
            # Nếu không có hôm qua, lấy ngày gần nhất có thể nhưng cảnh báo
            existing_dates = [c for c in df_combined.columns if 'Views_' in c and c != col_today]
            prev_data_col = sorted(existing_dates)[-1] if existing_dates else None

        df_report = df_today.copy()
        if prev_data_col:
            df_report[prev_data_col] = df_combined[prev_data_col].fillna(0)
            df_report['Views_Gained'] = df_report[col_today] - df_report[prev_data_col]
        else:
            df_report['Views_Gained'] = 0
            prev_data_col = 'N/A'
            df_report[prev_data_col] = 0

        # Lọc: Chỉ lấy video có views tăng
        df_report = df_report[df_report['Views_Gained'] > 0].copy()

        # Sắp xếp cột theo thứ tự A->G của bạn
        # A: ID, B: Title, C: Link, D: Publish, E: Gained, F: Yesterday, G: Today
        final_cols = ['Video ID', 'Title', 'Link', 'Publish Date', 'Views_Gained', prev_data_col, col_today]
        df_report = df_report[final_cols].sort_values(by='Views_Gained', ascending=False)

        # Thêm dòng TOTAL và Format
        def finalize_excel(df, filename, num_cols):
            if not df.empty:
                total_row = {col: '' for col in df.columns}
                total_row['Title'] = '🔥 TỔNG CỘNG'
                total_row['Video ID'] = 'TOTAL'
                for c in num_cols: 
                    if c in df.columns: total_row[c] = df[c].sum()
                df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)
                
                # Format dấu phẩy hàng ngàn
                for c in num_cols:
                    if c in df.columns:
                        df[c] = df[c].apply(lambda x: f"{int(x):,}" if isinstance(x, (int, float)) else x)

            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
                ws = writer.sheets['Sheet1']
                for col in ws.columns:
                    max_len = max([len(str(cell.value)) for cell in col]) + 2
                    ws.column_dimensions[col[0].column_letter].width = min(max_len, 70)
        
        finalize_excel(df_report, report_file, ['Views_Gained', prev_data_col, col_today])
        print(f"✅ Báo cáo đã sẵn sàng: {report_file}")

if __name__ == "__main__":
    API_KEY = os.environ.get('API_KEY') or os.environ.get('YOUTUBE_API_KEY')
    if not API_KEY: exit(1)

    tool = YouTubeAnalyticsTool(API_KEY)
    channel_id = tool.get_channel_id_by_handle('@stoicether')
    
    if channel_id:
        u_id = tool.get_uploads_playlist_id(channel_id)
        data = tool.get_all_videos_stats(u_id)
        # Xuất 2 tệp: 1 để bot lưu lịch sử, 1 để gửi email
        tool.generate_strict_report(data, "history_stoicether.xlsx", "report_views_increased.xlsx")
