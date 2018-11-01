import pandas as pd
import cv2
from operaDB import OperaDB

class OperaImage:
    def __init__(self, operaDB):
        self.opera = operaDB
        self.tables = 'driveragent_videofile'

    def get_VideoLists(self, df_tripLists, l):
        #Get video file name
        query_videofile = 'SELECT * FROM ' + self.tables
        query = query_videofile + ' WHERE video_id = ' +  "'" + str(df_tripLists['id'][l]) + "'"

        column_names, df_videoLists = self.opera.get_DataFrame( query )
        #print(column_names)
        #print('video file num : ' + str(len(df_videoLists)))
        return df_videoLists['filename']

    def get_VideoCapture(self, df_tripLists, l):
        self.start_datetime, self.stop_datetime = self.opera.get_TripTime(df_tripLists, l)
        videoLists = self.get_VideoLists(df_tripLists, l)
        print(videoLists)
        user_id = df_tripLists['user_id'][l]
        id = str(df_tripLists['id'][l])
        #
        videoFrontFilename = str(self.start_datetime) + ".mp4"

        videoFrontCap = None
        videoRoomCap = None
        if( 0 < len(videoLists) and len(videoLists) < 3 ):
            for vname in videoLists:
                #front camera
                if( vname == videoFrontFilename ):
                    videofile = self.opera.s3dir + "/" + user_id + "/" + id + "/" + vname
                    videoFrontCap = cv2.VideoCapture(videofile)
                    print('open front video file : ' + str(videoFrontCap.isOpened() ) )

                else:
                    videofile = self.opera.s3dir + "/" + user_id + "/" + id + "/" + vname
                    videoRoomCap = cv2.VideoCapture(videofile)
                    print('open room video file : ' + str(videoRoomCap.isOpened() ) )

        else:
            print("a number of vide file is fail, 1 or 2 is correct : " + str( len(videLists) ))

        return videoFrontCap, videoRoomCap

    def read(self, cap):
        ret, frame = cap.read()
        if( ret == True ):
            frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        return ret, frame

    def get_TimeStamp(self, cap):
        return self.start_datetime + int(cap.get(cv2.CAP_PROP_POS_MSEC))

    def get_Width(self, cap):
        return int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))

    def get_Height(self, cap):
        return int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    def get_FPS(self, cap):
        return float(cap.get(cv2.CAP_PROP_FPS))

    def get_SizeFrameCount(self, cap):
        return int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    def get_FrameCount(self, cap):
        return int(cap.get(cv2.CAP_PROP_POS_FRAMES))

    def set_FrameCount(self, cap):
        return int(cap.set(cv2.CAP_PROP_POS_FRAMES))

    #未実装
    #cv2.CAP_PROP_POS_AVI_RATIO 現在のフレームの相対的な位置 #値がおかしいので実装してない
    #cv2.CAP_PROP_FOURCC コーデックを表す4文字コード
    #cv2.CAP_PROP_FORMAT retrieve() によって返されるMat オブジェクトのフォーマット
    #cv2.CAP_PROP_MODE 現在のキャプチャモードを表す、バックエンド固有の値
    #cv2.CAP_PROP_BRIGHTNESS 画像の明るさ（カメラの場合のみ）
    #cv2.CAP_PROP_CONTRAST 画像のコントラスト（カメラの場合のみ）
    #cv2.CAP_PROP_SATURATION 画像の彩度（カメラの場合のみ）
    #cv2.CAP_PROP_HUE 画像の色相（カメラの場合のみ）
    #cv2.CAP_PROP_GAIN 画像のゲイン（カメラの場合のみ）
    #cv2.CAP_PROP_EXPOSURE 露出（カメラの場合のみ）
    #cv2.CAP_PROP_CONVERT_RGB 画像がRGBに変換されるか否かを表す、ブール値のフラグ
