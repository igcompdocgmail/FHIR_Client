function generateApiKey(length) {
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    const charactersLength = characters.length;
    let apiKey = '';
  
    for (let i = 0; i < length; i++) {
      apiKey += characters.charAt(Math.floor(Math.random() * charactersLength));
    }
  
    return apiKey;
  }
  
  // Usage example: generate a random API key with a length of 32 characters
  const randomApiKey = generateApiKey(32);
  console.log(randomApiKey);
  