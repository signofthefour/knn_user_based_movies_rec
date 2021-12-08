from flask import Flask, jsonify, request
from flask_restful import reqparse, abort, Api, Resource
from new_user_update import new_user
from database import get_cursor
from tqdm import tqdm

app = Flask(__name__)
api = Api(app)

#parser = reqparse.RequestParser()
#parser.add_argument('username', type=unicode, location='json')
#parser.add_argument('password', type=unicode, location='json')

class UpdateUser(Resource):
    def post(self):
        json_data = request.get_json(force=True)
        print(json_data)
        print("="*50)
        user_id = json_data["user_id"]
        age = json_data["age"]
        occupation = json_data["occupation"]
        gender = json_data["gender"]
        zip_code = json_data["zip_code"]
        res_data = new_user(user_id=int(user_id), age=int(age), occupation=int(occupation), gender=gender, zip_code=zip_code)
        explain = "Highly rated by most people like you"

        cursor, db_connection = get_cursor() 
        for pair in tqdm(res_data):
            db_connection.commit()
            cursor.execute("INSERT INTO UserMovieRec VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING", (int(pair[0]), int(pair[1]), 0, explain)) 
        return 200

api.add_resource(UpdateUser, '/update_user')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5444 ,debug=True)
