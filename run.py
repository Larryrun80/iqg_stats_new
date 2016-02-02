from stats import app

print(app.config['DEBUG'])


if __name__ == '__main__':
    app.run(host='0.0.0.0')
