from cryptography.fernet import Fernet

def database_decryption():

    key = Fernet.generate_key() #this is your "password"
    cipher_suite = Fernet(b'hfU4dswDc9ukztzbaG1JNThatWM1GPe5wFr_mL6NZOA=')
    decoded_text = cipher_suite.decrypt(b'gAAAAABgrI-gTyb81xh6LyVUzjDvoGkmeGAXWh_OCG4chjsx4GA-BYFkEZH7Ukw4CCoyKh1TIzwmj4AsVjkkUwOwRmwKhURDCQ==')
    return decoded_text.decode('utf-8')
