import os
import uuid
import pytesseract
from io import BytesIO
from PIL import Image, ImageOps, ImageFilter
from selenium.webdriver.remote.webdriver import WebDriver

from scraper.config.logging import StructuredLogger
from scraper.utils.exceptions import OCRException


def save_image(logger: StructuredLogger, image: Image.Image | bytes, ocr_target: str, stage: str) -> None:
    try:
        random_uuid = uuid.uuid4().hex[:8]  # Generate a random 8-character UUID
        filename = f"{ocr_target}_{stage}_{random_uuid}.png"
        save_dir = os.path.join(os.getcwd(), f"files/data/output/temp_ocr_out/{ocr_target}")
        os.makedirs(save_dir, exist_ok=True)  # Ensure the directory exists
        save_path = os.path.join(save_dir, filename)
        if isinstance(image, bytes):
            png_image = Image.open(BytesIO(image))
            png_image.save(save_path)
        else:
            image.save(save_path)
    except Exception as e:
        logger.error(f"Failed to save {ocr_target} {stage} image: {e}", exc_info=True)
        raise OSError(f"Failed to save {ocr_target} {stage} ocr image")


def preprocess_rating(logger: StructuredLogger, raw_image: bytes) -> Image.Image:
    try:
        image = Image.open(BytesIO(raw_image))
        # Define cropping box for the rating image (39x14)
        left, upper, right, lower = (960 - 19, 497 - 6, 960 + 20, 497 + 7)
        cropped_image = image.crop((left, upper, right, lower))
        # Resize the image to enhance OCR accuracy
        resized_image = cropped_image.resize((cropped_image.width * 3, cropped_image.height * 3))
        # filter resized image
        # median_filtered_image = resized_image.filter(ImageFilter.MedianFilter(size=3))
        # convert to grayscale prior to autocontrast
        grayscale_image = ImageOps.grayscale(resized_image)
        # Increase contrast
        contrast_image = ImageOps.autocontrast(grayscale_image, cutoff=0)
        # Thresholding
        threshold = 200
        processed_image = contrast_image.point(lambda p: 255 if p > threshold else 0)
        return processed_image
    except Exception as e:
        logger.error(f"Failed to process rating: {e}", exc_info=True)
        raise OCRException


def rating_ocr(logger: StructuredLogger, link: str, ocr_driver: WebDriver) -> str:
    try:
        ocr_driver.get(link)
        raw_image = ocr_driver.get_screenshot_as_png()
        processed_image = preprocess_rating(logger, raw_image)
        # save_image(logger, processed_image, ocr_target="rating", stage="processed")
        rating_config = r"--psm 7 -c tessedit_char_whitelist=ABCNRWatl123+-"
        text = pytesseract.image_to_string(processed_image, config=rating_config)
        out = str(text).replace("t", "+").replace("l", "1").replace("++", "+")
        # logger.info(f"Extracted {out.strip()} from {link}")
        return out.strip()
    except Exception as e:
        logger.error(f"OCR on {link} failed: {e}", exc_info=True)
        return "OCR Failure"


def preprocess_cusip(logger: StructuredLogger, raw_image: bytes) -> Image.Image:
    try:
        image = Image.open(BytesIO(raw_image))
        # Define cropping box
        left, upper, right, lower = (960 - 50, 497 - 30, 960 + 50, 497 + 30)
        cropped_image = image.crop((left, upper, right, lower))
        resized_image = cropped_image.resize((cropped_image.width * 3, cropped_image.height * 3))
        median_filtered_image = resized_image.filter(ImageFilter.MedianFilter(size=3))
        grayscale_image = ImageOps.grayscale(median_filtered_image)
        # Increase contrast
        contrast_image = ImageOps.autocontrast(grayscale_image, cutoff=0)
        # Thresholding
        threshold = 200  # Adjust this value based on your images
        processed_image = contrast_image.point(lambda p: 255 if p > threshold else 0)
        return processed_image
    except Exception as e:
        logger.error(f"Failed to process cusip: {e}", exc_info=True)
        raise OCRException


def cusip_ocr(logger: StructuredLogger, link: str, ocr_driver: WebDriver) -> str:
    try:
        ocr_driver.get(link)
        raw_image = ocr_driver.get_screenshot_as_png()
        processed_image = preprocess_cusip(logger, raw_image)
        # save_image(logger, processed_image, ocr_target="cusip", stage="processed")
        alphanumeric_config = r"--psm 7 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        text = pytesseract.image_to_string(processed_image, config=alphanumeric_config)
        # logger.info(f"Extracted {text.strip()} from {link}")
        return text.strip()
    except Exception as e:
        logger.error(f"OCR on {link} failed: {e}", exc_info=True)
        return "OCR Failure"
