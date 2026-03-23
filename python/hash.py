from PIL import Image
import imagehash

def generate_hash(frame):
    img = Image.fromarray(frame)
    return str(imagehash.phash(img))