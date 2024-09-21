from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time

# 提取重复的 try-except 结构到一个函数
def safe_get_element_text(driver, xpath):
    try:
        element = wait_for_element(driver, xpath)
        return element.text if element else None
    except Exception as e:
        print(f"Error in getting text for xpath {xpath}: {e}")
        return None

def safe_get_element_attribute(driver, xpath, attribute):
    try:
        element = wait_for_element(driver, xpath)
        return element.get_attribute(attribute) if element else None
    except Exception as e:
        print(f"Error in getting attribute '{attribute}' for xpath {xpath}: {e}")
        return None

def try_click_element(driver, element, description="element"):
    """尝试点击一个元素，并捕获可能的异常。"""
    try:
        element.click()
        time.sleep(1)  # 等待页面刷新或加载
    except Exception as e:
        print(f"Error clicking {description}: {e}")

    
def wait_for_element(driver, xpath, timeout=5):
    """Wait for an element to be present and return it."""
    try:
        return WebDriverWait(driver, timeout).until(
            EC.visibility_of_element_located((By.XPATH, xpath))
        )
    except TimeoutException:
        return None