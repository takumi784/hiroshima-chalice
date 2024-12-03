import boto3, json
from datetime import datetime, timedelta
from chalice import Chalice
from chalicelib.receiver_util import decimal_to_int, create_audio_sample, get_simple_blocks, get_media_data, convert_bytearray_to_wav, upload_audio_to_s3

app = Chalice(app_name='hiroshima-funcs')

@app.lambda_function(name='receiver')
def receiver(event, context):
    # TODO:ここでKVSからデータを取得する処理をかく
    # TODO: ここでS3(bucketName: connect-voice, prefix: /row)にデータをアップロードする処理を書く
    try:
      JST_OFFSET = timedelta(hours=9)
      print('Received event:' + json.dumps(event,default=decimal_to_int, ensure_ascii=False))
      media_streams = event["Details"]["ContactData"]["MediaStreams"]["Customer"]["Audio"]
      stream_arn = media_streams["StreamARN"]
      start_time = float(media_streams["StartTimestamp"]) / 1000
      end_time = float(media_streams["StopTimestamp"]) / 1000
      combined_samples = create_audio_sample(
          get_simple_blocks(
            get_media_data(stream_arn, start_time, end_time)
          )
        )

      wav_audio = convert_bytearray_to_wav(combined_samples)
      bucket_name = "test-bucket-for-connect" #後にconnect-voice に切り替え
      jst_time = datetime.utcnow() + JST_OFFSET
      filename = "row/customer_voice_{date}.wav".format(date=jst_time.strftime("%Y%m%d_%H%M%S"))

      upload_success = upload_audio_to_s3(bucket_name, wav_audio, filename)
      
    except Exception as e:
        print("Error", e)

    return {"hello": "world"}
