import os
import google.generativeai as genai

# Set up environment variable with the API key if you running locally
# genai.configure(api_key=os.environ["GOOGLE_API_KEY"])

# Gemini settings to manipulate a desired outcome
generation_config = {
    "temperature": 1,
    "top_p": 0.95,
    "top_k": 64,
    "max_output_tokens": 9000,
    "response_mime_type": "text/plain",
}
safety_settings = [
    {
        "category": "HARM_CATEGORY_HARASSMENT",
        "threshold": "BLOCK_MEDIUM_AND_ABOVE",
    },
    {
        "category": "HARM_CATEGORY_HATE_SPEECH",
        "threshold": "BLOCK_MEDIUM_AND_ABOVE",
    },
    {
        "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
        "threshold": "BLOCK_MEDIUM_AND_ABOVE",
    },
    {
        "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
        "threshold": "BLOCK_MEDIUM_AND_ABOVE",
    },
]

model = genai.GenerativeModel(
    model_name="gemini-pro",
    generation_config=generation_config,
)

chat_session = model.start_chat(
    history=[
    ]
)

def establish_api(key: str) -> str:
    """
    Establishes a connection with the Gemini API using the provided key. 
    A lot of blood, sweat, and tears went into writing this function.

    Args:
        key (str): The Gemini API key obtained from the user.
    Returns:
        str: Feedback message indicating successful or unsuccessful API connection (optional).
    """
    genai.configure(api_key=key)
    return "Key inserted successfully!"

def send_prompt(ai_prompt: str, prompt: str) -> str:
    """
    Sends a user prompt to the Gemini API and retrieves the response.
    *Cracks knuckles* That's a job well done for today..

    Args:
        ai_prompt (str): The AI prompt to send to the Gemini API.
        prompt (str): The user's message input.
    Returns:
        str: The response received from the Gemini API.
    """
    try:
        # full_prompt = prompt + "\n" + ai_prompt
        
        response = model.generate_content([ai_prompt[0], prompt])
        # response = chat_session.send_message(full_prompt)
        return response.text
    except Exception as e:
        return f"Sorry, but you need to insert API key to start conversation. Error: {str(e)}"