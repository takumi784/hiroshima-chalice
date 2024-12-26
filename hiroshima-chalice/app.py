import os
import json
from datetime import datetime, timedelta

import time
import random

import ffmpeg

import uuid
import boto3
from boto3.dynamodb.conditions import Key
from chalice import Chalice
from chalice.app import DynamoDBEvent

from chalicelib.receiver_util import (convert_bytearray_to_wav,
                                      create_audio_sample, decimal_to_int,
                                      get_media_data, get_simple_blocks,
                                      upload_audio_to_s3)
from chalicelib.helper.log_elapsed_time import log_elapsed_time


app = Chalice(app_name='hiroshima-funcs')

dynamodb = boto3.resource('dynamodb')
basic_table = dynamodb.Table('hiroshima-table-test')
analyze_table = dynamodb.Table('hiroshima-analyze-test')

@app.lambda_function(name='receiver')
def receiver(event, context):
    recordId = uuid.uuid4()
    try:
        # KVS から取得
        JST_OFFSET = timedelta(hours=9)
        print('Received event:' + json.dumps(event,default=decimal_to_int, ensure_ascii=False))
        start_time = time.time()
        media_streams = event["Details"]["ContactData"]["MediaStreams"]["Customer"]["Audio"]
        stream_arn = media_streams["StreamARN"]
        media_start_time = float(media_streams["StartTimestamp"]) / 1000
        media_end_time = float(media_streams["StopTimestamp"]) / 1000
        combined_samples = create_audio_sample(
            get_simple_blocks(
                get_media_data(stream_arn, media_start_time, media_end_time)
            )
        )

        log_elapsed_time("receiver: created audio sample", start_time)

        # s3://test-bucket-for-connect/row/customer_voice_{date}.wav として保存
        wav_audio = convert_bytearray_to_wav(combined_samples)
        bucket_name = "test-bucket-for-connect"
        jst_time = datetime.utcnow() + JST_OFFSET
        filename = "row/customer_voice_{date}.wav".format(date=jst_time.strftime("%Y%m%d_%H%M%S"))

        upload_success = upload_audio_to_s3(bucket_name, wav_audio, filename)
      
        log_elapsed_time("receiver: upload audio", start_time)

        file_path = f"s3://{bucket_name}/{filename}"

        response = basic_table.put_item(
            Item={
                'uuid': str(recordId),
                'row_file_path': file_path
            }
        )
        print(response)
        log_elapsed_time("receiver: put table", start_time)

        return {
            "statusCode": 200,
            "recordId": str(recordId)
        }

    except Exception as e:
        basic_table.put_item(
            Item={
                'uuid': str(recordId),
                'row_file_path': "failed_upload"
            }
        )
        print("Error", e)
        return {
            "statusCode": 500,
            "message": str(e)
        }

def convert_to_8k_ulaw_wav(input_audio_bytes):
    """
    Convert PCM audio to 8kHz μ-law WAV format using ffmpeg
    """
    ffmpeg_path = '/opt/ffmpeg' # Lambda Layer に展開された ffmpeg のパス

    try:
        stream = (
            ffmpeg
            .input('pipe:', format='s16le', ar='8000', ac=1)  # PCM 16-bit little-endian
            .output('pipe:', 
                acodec='pcm_mulaw',
                ar=8000,
                ac=1,
                f='wav'
            )
        )
        
        out, _ = ffmpeg.run(stream, 
            input=input_audio_bytes,
            capture_stdout=True,
            capture_stderr=True,
            cmd=ffmpeg_path
        )
        
        return out
        
    except ffmpeg.Error as e:
        print(f'FFmpeg error occurred: {e.stderr.decode() if e.stderr else str(e)}')
        raise

# NOTE: これはDynamoDBのconsoleからStreamIDを取得してコピペする。
@app.on_dynamodb_record(
    stream_arn="arn:aws:dynamodb:ap-northeast-1:183295409111:table/hiroshima-table-test/stream/2024-12-16T15:47:31.011")
def voiceAnalyzer(event: DynamoDBEvent):
    start_time = time.time()
    record_data = event.to_dict()['Records'][0]
    print(record_data)

    # INSERT 以外で起動しない
    if record_data['eventName'] != 'INSERT':
        print("Skipping event")
        return {"error": "not INSERT"}
    
    uuid = record_data["dynamodb"]["Keys"]["uuid"]["S"]
    s3_row_file_path = record_data["dynamodb"]["Keys"]["row_file_path"]["S"]

    JST_OFFSET = timedelta(hours=9)
    jst_time = datetime.utcnow() + JST_OFFSET

    s3_response_audio_path = 'responses/ai_voice_{date}.wav'.format(date=jst_time.strftime("%Y%m%d_%H%M%S")) # S3 に保存するときのパス
    full_s3_response_audio_path = f's3://test-bucket-for-connect/{s3_response_audio_path}' # Dyanmo に書き込むときのパス

    try:
        #解析開始: ANALYZING
        item={
            'uuid': uuid,
            'analyze_file_path': full_s3_response_audio_path,
            'row_file_path': s3_row_file_path,
            'status': 'ANALYZING',
            'is_conversation_finished': False
        }
        analyze_table.put_item(Item=item)
        print(f"Writing to DynamoDB table: hiroshima-table-test")
        print(f"Data: {json.dumps(item, ensure_ascii=False)}")

        s3_bucket, s3_key = s3_row_file_path.replace("s3://", "").split("/", 1)

        # 文字起こし
        transcribe = boto3.client('transcribe')
        transcription_job_name = "transcription-job-{date}".format(date=jst_time.strftime("%Y%m%d_%H%M%S"))
        
        transcribe.start_transcription_job(
            TranscriptionJobName=transcription_job_name,
            LanguageCode='ja-JP',
            MediaFormat=os.path.splitext(s3_key)[1][1:], #拡張子
            Media={
                'MediaFileUri': f's3://{s3_bucket}/{s3_key}' #ローカルファイルは使えない 
            },
            OutputBucketName='test-bucket-for-connect',
            OutputKey=f'transcriptions/{transcription_job_name}.json'
        )
        
        log_elapsed_time(f"analyzer: Transcription job {transcription_job_name} started", start_time)

        # 返答生成
        # Transcribeジョブの結果を待つ
        while True:
            response = transcribe.get_transcription_job(TranscriptionJobName=transcription_job_name)
            if response['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
                break
            time.sleep(1)  # 1秒ごとにチェック
            log_elapsed_time(f"analyzer: waiting Transcription job...", start_time)

        # 文字起こし結果を取得
        if response['TranscriptionJob']['TranscriptionJobStatus'] == 'COMPLETED':
            s3 = boto3.client('s3')
            transcription_object = s3.get_object(
                Bucket='test-bucket-for-connect', 
                Key=f'transcriptions/{transcription_job_name}.json'
            )

            # 文字起こしテキストを抽出
            transcription_data = json.loads(transcription_object['Body'].read().decode('utf-8'))
            transcribed_text = transcription_data['results']['transcripts'][0]['transcript']

            print(transcribed_text)
            log_elapsed_time(f"analyzer: got transcription_data", start_time)

            # Bedrock (Anthropic Claude)で返答を生成
            bedrock_client = boto3.client('bedrock-runtime')

            prompt = f"""会話の文脈：{transcribed_text}

あなたは、この音声メッセージに対して自然な続きの返答を1行で生成してください。"""

            accept = "application/json"
            content_type = "application/json"

            body = json.dumps({
                "inputText": prompt,
                "textGenerationConfig": {
                    "maxTokenCount": 300,
                    "stopSequences": [],
                    "temperature": 0.5,
                    "topP": 0.9
                },
            })

            log_elapsed_time(f"analyzer: invoke model", start_time)
            response = bedrock_client.invoke_model(
                modelId="amazon.titan-text-express-v1",
                body=body,
                accept=accept, 
                contentType=content_type
            )

            # レスポンスから返答を抽出
            response_body = json.loads(response.get("body").read())
            for result in response_body['results']:
                generated_response = result['outputText']
                print(generated_response)
        
        # 会話終了判定（検証のため False に）
        is_conversation_finished = False


        # 音声合成
        polly_client = boto3.client('polly')
    
        # Pollyで音声合成 (16bit PCM, 8kHz)
        response = polly_client.synthesize_speech(
            Text=generated_response,
            OutputFormat='pcm',
            SampleRate='8000',
            VoiceId='Mizuki',    # 日本語の音声
            LanguageCode='ja-JP'
        )

        log_elapsed_time(f"analyzer: synthesized speech", start_time)
    
        # 音声データを取得
        pcm_data = response['AudioStream'].read()

        # PCMをWAVに変換
        wav_data = convert_to_8k_ulaw_wav(pcm_data)
        log_elapsed_time(f"analyzer: processed audio", start_time)
    
        # S3に保存
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket='test-bucket-for-connect', 
            Key=s3_response_audio_path, 
            Body=wav_data,
            ContentType='audio/wav'
        )

        log_elapsed_time(f"analyzer:put audio object", start_time)

        # テーブル更新: ANALYZED
        analyze_item  = {
            'uuid': uuid,
            'analyze_file_path': full_s3_response_audio_path,
            'row_file_path': s3_row_file_path,
            'status': 'ANALYZED',
            'is_conversation_finished': is_conversation_finished
        }
        response = analyze_table.put_item(Item=analyze_item)
        print(f"Writing to DynamoDB table: hiroshima-analyze-test")
        print(f"Data: {json.dumps(analyze_item, ensure_ascii=False)}")

        log_elapsed_time(f"analyzer: analyzed and put table", start_time)
            
    except Exception as e:
        print("Error", e)

    return {"hello": "world"}

@app.lambda_function(name='lexAnalyzer')
def lexAnalyzer(event, context):
    print("Received event:" + json.dumps(event, default=decimal_to_int, ensure_ascii=False))
    start_time = time.time()
    JST_OFFSET = timedelta(hours=9)
    jst_time = datetime.utcnow() + JST_OFFSET

    recordId = uuid.uuid4() # 今回の解析で使う id

    s3_response_audio_path = 'responses/ai_voice_{date}.wav'.format(date=jst_time.strftime("%Y%m%d_%H%M%S")) # S3 に保存するときのパス
    full_s3_response_audio_path = f's3://test-bucket-for-connect/{s3_response_audio_path}' # Dyanmo に書き込むときのパス

    connect_contact_id = event["sessionId"] # Connect のコンタクトid を取得
    transcript_text = event["inputTranscript"] # 文字起こしを取得


    try:
        #解析開始: ANALYZING
        item={
            'uuid': str(recordId),
            'analyze_file_path': full_s3_response_audio_path,
            'status': 'ANALYZING',
            'is_conversation_finished': False
        }
        analyze_table.put_item(Item=item)
        print(f"Writing to DynamoDB table: hiroshima-table-test")
        print(f"Data: {json.dumps(item, ensure_ascii=False)}")

        # Connect フローに recordId をコンタクト属性として渡す
        connect_client = boto3.client('connect')
        response = connect_client.update_contact_attributes(
            InstanceId="arn:aws:connect:ap-northeast-1:183295409111:instance/51acae49-08f5-4ffc-bee1-7166f30e9fb6",
            InitialContactId=connect_contact_id,
            Attributes={
                'recordId': str(recordId)
            }
        )
        print(f"UpdateContactAttributes response: {response}")

        # Bedrock (Anthropic Claude)で返答を生成
        bedrock_client = boto3.client('bedrock-runtime')

        prompt = f"""会話の文脈：{transcript_text}

あなたは、この音声メッセージに対して自然な続きの返答を1行で生成してください。"""

        accept = "application/json"
        content_type = "application/json"

        body = json.dumps({
            "inputText": prompt,
            "textGenerationConfig": {
                "maxTokenCount": 300,
                "stopSequences": [],
                "temperature": 0.5,
                "topP": 0.9
            },
        })

        log_elapsed_time(f"analyzer: invoke model", start_time)
        response = bedrock_client.invoke_model(
            modelId="amazon.titan-text-express-v1",
            body=body,
            accept=accept, 
            contentType=content_type
        )

        # レスポンスから返答を抽出
        response_body = json.loads(response.get("body").read())
        for result in response_body['results']:
            generated_response = result['outputText']
            print(generated_response)
    
        # 会話終了判定（検証のため False に）
        is_conversation_finished = False

        # 音声合成
        polly_client = boto3.client('polly')
    
        # Pollyで音声合成 (16bit PCM, 8kHz)
        response = polly_client.synthesize_speech(
            Text=generated_response,
            OutputFormat='pcm',
            SampleRate='8000',
            VoiceId='Mizuki',    # 日本語の音声
            LanguageCode='ja-JP'
        )

        log_elapsed_time(f"analyzer: synthesized speech", start_time)
    
        # 音声データを取得
        pcm_data = response['AudioStream'].read()

        # PCMをWAVに変換
        wav_data = convert_to_8k_ulaw_wav(pcm_data)
        log_elapsed_time(f"analyzer: processed audio", start_time)
    
        # S3に保存
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket='test-bucket-for-connect', 
            Key=s3_response_audio_path, 
            Body=wav_data,
            ContentType='audio/wav'
        )

        log_elapsed_time(f"analyzer:put audio object", start_time)

        # テーブル更新: ANALYZED
        analyze_item  = {
            'uuid': str(recordId),
            'analyze_file_path': full_s3_response_audio_path,
            'status': 'ANALYZED',
            'is_conversation_finished': is_conversation_finished
        }
        response = analyze_table.put_item(Item=analyze_item)
        print(f"Writing to DynamoDB table: hiroshima-analyze-test")
        print(f"Data: {json.dumps(analyze_item, ensure_ascii=False)}")

        log_elapsed_time(f"analyzer: analyzed and put table", start_time)

    except Exception as e:
        print("Error", e)

    #Lex ボットを終了させる
    return {
        "sessionState": {
            "dialogAction": {
                "type": "Close",
            },
            "intent": {
                "name": "OneVoice",
                "state": "Fulfilled"
            }
        },
        "messages": [
                {
                    "contentType": "PlainText",
                    "content": ","
                }
            ]
    }

@app.lambda_function(name='simpler-lexAnalyzer')
def simplerLexAnalyzer(event, context):
    print("Received event:" + json.dumps(event, default=decimal_to_int, ensure_ascii=False))
    start_time = time.time()
    JST_OFFSET = timedelta(hours=9)
    jst_time = datetime.utcnow() + JST_OFFSET

    s3_response_audio_path = 'responses/ai_voice_{date}.wav'.format(date=jst_time.strftime("%Y%m%d_%H%M%S")) # S3 に保存するときのパス
    full_s3_response_audio_path = f's3://test-bucket-for-connect/{s3_response_audio_path}' # Dyanmo に書き込むときのパス

    connect_contact_id = event["sessionId"] # Connect のコンタクトid を取得
    transcript_text = event["inputTranscript"] # 文字起こしを取得


    try:
        # Bedrock (Anthropic Claude)で返答を生成
        bedrock_client = boto3.client('bedrock-runtime')

        prompt = f"""会話の文脈：{transcript_text}

あなたは、この音声メッセージに対して自然な続きの返答を1行で生成してください。"""

        accept = "application/json"
        content_type = "application/json"

        body = json.dumps({
            "inputText": prompt,
            "textGenerationConfig": {
                "maxTokenCount": 300,
                "stopSequences": [],
                "temperature": 0.5,
                "topP": 0.9
            },
        })

        log_elapsed_time(f"analyzer: invoke model", start_time)
        response = bedrock_client.invoke_model(
            modelId="amazon.titan-text-express-v1",
            body=body,
            accept=accept, 
            contentType=content_type
        )

        # レスポンスから返答を抽出
        response_body = json.loads(response.get("body").read())
        for result in response_body['results']:
            generated_response = result['outputText']
            print(generated_response)
    
        # 会話終了判定（検証のため False に）
        is_conversation_finished = False

        # 音声合成
        polly_client = boto3.client('polly')
    
        # Pollyで音声合成 (16bit PCM, 8kHz)
        response = polly_client.synthesize_speech(
            Text=generated_response,
            OutputFormat='pcm',
            SampleRate='8000',
            VoiceId='Mizuki',    # 日本語の音声
            LanguageCode='ja-JP'
        )

        log_elapsed_time(f"analyzer: synthesized speech", start_time)
    
        # 音声データを取得
        pcm_data = response['AudioStream'].read()

        # PCMをWAVに変換
        wav_data = convert_to_8k_ulaw_wav(pcm_data)
        log_elapsed_time(f"analyzer: processed audio", start_time)
    
        # S3に保存
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket='test-bucket-for-connect', 
            Key=s3_response_audio_path, 
            Body=wav_data,
            ContentType='audio/wav'
        )

        log_elapsed_time(f"analyzer:put audio object", start_time)

        # is_conversation_finished, analyze_file_path をコンタクト属性としてフローに渡す
        connect_client = boto3.client('connect')
        response = connect_client.update_contact_attributes(
            InstanceId="arn:aws:connect:ap-northeast-1:183295409111:instance/51acae49-08f5-4ffc-bee1-7166f30e9fb6",
            InitialContactId=connect_contact_id,
            Attributes={
                'is_conversation_finished': str(is_conversation_finished),
                'analyze_file_path': full_s3_response_audio_path
            }
        )
        print(f"UpdateContactAttributes response: {response}")

    except Exception as e:
        print("Error", e)

    #Lex ボットを終了させる
    return {
        "sessionState": {
            "dialogAction": {
                "type": "Close",
            },
            "intent": {
                "name": "OneVoice",
                "state": "Fulfilled"
            }
        },
        "messages": [
                {
                    "contentType": "PlainText",
                    "content": ","
                }
            ]
    }

@app.lambda_function(name="poller")
def poller(event, context):
    print(event)
    # Connect から受け取った recordId
    dynamoDBRecordId = event["Details"]["Parameters"]["recordId"]
    
    try:
        # query() を使用して指定の UUID のレコードを検索
        response = analyze_table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('uuid').eq(dynamoDBRecordId)
        )

        print(response)

        # クエリ結果が存在するかチェック
        if response['Count'] > 0:
            # 最初の（唯一の）アイテムを取得
            item = response['Items'][0]

            # 返却するデータ構造を準備
            result = {
                # status が存在しない場合はデフォルトで NOT_FOUND を設定
                'status': item.get('status', 'NOT_FOUND'),
                # is_conversation_finished が存在しない場合はデフォルトで False を設定
                'is_conversation_finished': str(item.get('is_conversation_finished', False)),
                # analyze_file_path が存在しない場合は空文字列
                'analyze_file_path': item.get('analyze_file_path', '')
            }

            return result
        else:
            # レコードが見つからない場合
            return {
                'status': 'NOT_FOUND',
                'is_conversation_finished': 'False',
                'analyze_file_path': ''
            }

    except Exception as e:
        print(f"Error in poller: {e}")
        return {
            'status': 'ERROR',
            'is_conversation_finished': 'False',
            'analyze_file_path': ''
        }
