FROM node:20-alpine

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install --production

# Copy application code
COPY . .

# Copy active_markets.json from website data (for market list)
# This will be mounted or copied during deployment

EXPOSE 8080

CMD ["node", "live-data-server.js"]
