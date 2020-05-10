from app import create_app

if __name__ == '__main__':
    app_ins = create_app()
    app_ins.run(port=8080, debug=True)

