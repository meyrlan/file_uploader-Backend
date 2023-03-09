import os
import boto3
from flask import Flask, jsonify, make_response, request
import uuid
import threading

app = Flask(__name__)
bucket_name = os.getenv('BUCKET_NAME')
access_key = os.getenv('ACCESS_KEY')
secret_key = os.getenv('SECRET_KEY')
s3 = boto3.client('s3', aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key)


def upload_part(file, bucket_name, filename, upload_id, part_number, part_size):
    part_data = file.read(part_size)
    response = s3.upload_part(Bucket=bucket_name, Key=filename,
                              PartNumber=part_number, UploadId=upload_id,
                              Body=part_data)
    return {'PartNumber': part_number, 'ETag': response['ETag']}


@app.route('/partial_upload', methods=['POST'])
def partial_upload():
    file = request.files['file']
    file_extension = os.path.splitext(file.filename)[1]
    filename = str(uuid.uuid4()) + file_extension
    try:
        multipart_upload = s3.create_multipart_upload(
            Bucket=bucket_name, Key=filename)
        upload_id = multipart_upload['UploadId']

        part_size = 5 * 1024 * 1024  # 5 MB
        file_size = os.fstat(file.fileno()).st_size
        parts = []
        threads = []

        for i in range(0, file_size, part_size):
            part_number = i // part_size + 1
            t = threading.Thread(target=upload_part,
                                 args=(file, bucket_name, filename, upload_id, part_number, part_size))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        response = s3.list_parts(Bucket=bucket_name, Key=filename,
                                 UploadId=upload_id)
        parts = [{'PartNumber': part['PartNumber'], 'ETag': part['ETag']}
                 for part in response['Parts']]

        s3.complete_multipart_upload(Bucket=bucket_name, Key=filename,
                                     UploadId=upload_id, MultipartUpload={'Parts': parts})

        file_url = f"https://{bucket_name}.s3.amazonaws.com/{filename}"
        response = make_response(
            jsonify({"message": "File uploaded successfully", "url": file_url}), 200)
    except Exception as ex:
        response = make_response(
            jsonify({"message": str(ex)}), 400)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


@app.route('/whole_upload', methods=['POST'])
def whole_upload():
    file = request.files['file']
    file_extension = os.path.splitext(file.filename)[1]
    filename = str(uuid.uuid4()) + file_extension
    try:
        s3.upload_fileobj(file, bucket_name, filename)
        file_url = f"https://{bucket_name}.s3.amazonaws.com/{filename}"
        response = make_response(
            jsonify({"message": "File uploaded successfully", "url": file_url}), 200)
    except Exception as ex:
        response = make_response(
            jsonify({"message": str(ex)}), 400)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


if __name__ == '__main__':
    app.run(debug=True)
