const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
    if (req.url === '/') {
        fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
            if (err) {
                res.writeHead(500);
                return res.end('Error loading index.html');
            }
            res.writeHead(200, { 'Content-Type': 'text/html' });
            res.end(data);
        });
    } else {
        res.writeHead(404);
        res.end('Not found');
    }
});

const wss = new WebSocket.Server({ server });

const messageHistory = {};
const connectedUsers = {};

wss.on('connection', (ws) => {
    let currentUser = null;
    let currentServerId = null;
    let currentChannelId = null;

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            
            switch (data.type) {
                case 'join':
                    currentUser = data.user;
                    currentServerId = data.serverId;
                    currentChannelId = data.channelId;
                    
                    // Create unique ID for this user connection
                    const userId = currentUser.id;
                    
                    // Store the connection
                    if (!connectedUsers[currentServerId]) {
                        connectedUsers[currentServerId] = {};
                    }
                    if (!connectedUsers[currentServerId][currentChannelId]) {
                        connectedUsers[currentServerId][currentChannelId] = {};
                    }
                    connectedUsers[currentServerId][currentChannelId][userId] = {
                        ws: ws,
                        user: currentUser
                    };
                    
                    // Send message history for this channel
                    const historyKey = `${currentServerId}-${currentChannelId}`;
                    if (!messageHistory[historyKey]) {
                        messageHistory[historyKey] = [];
                    }
                    
                    ws.send(JSON.stringify({
                        type: 'history',
                        serverId: currentServerId,
                        channelId: currentChannelId,
                        messages: messageHistory[historyKey]
                    }));
                    
                    // Send system message about new user joining
                    const joinMessage = {
                        id: Math.random().toString(36).substr(2, 9),
                        content: `${currentUser.username} has joined the channel`,
                        author: { 
                            id: 'system', 
                            username: 'System', 
                            avatar: 'S' 
                        },
                        timestamp: new Date().toISOString(),
                        system: true
                    };
                    
                    // Broadcast to all users in the channel
                    broadcastToChannel(currentServerId, currentChannelId, {
                        type: 'message',
                        serverId: currentServerId,
                        channelId: currentChannelId,
                        message: joinMessage
                    });
                    
                    // Send current online users list
                    sendOnlineUsers(currentServerId, currentChannelId);
                    
                    console.log(`User ${currentUser.username} joined server ${currentServerId}, channel ${currentChannelId}`);
                    break;
                    
                case 'message':
                    if (!currentUser) {
                        // User hasn't joined a channel yet
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Please join a channel first'
                        }));
                        return;
                    }
                    
                    const messageKey = `${data.serverId}-${data.channelId}`;
                    
                    // Save message to history
                    if (!messageHistory[messageKey]) {
                        messageHistory[messageKey] = [];
                    }
                    messageHistory[messageKey].push(data.message);
                    
                    // Limit history to 100 messages per channel
                    if (messageHistory[messageKey].length > 100) {
                        messageHistory[messageKey] = messageHistory[messageKey].slice(-100);
                    }
                    
                    // Broadcast to all users in the channel
                    broadcastToChannel(data.serverId, data.channelId, {
                        type: 'message',
                        serverId: data.serverId,
                        channelId: data.channelId,
                        message: data.message
                    });
                    
                    console.log(`Message from ${data.message.author.username} in ${data.serverId}/${data.channelId}: ${data.message.content.substring(0, 50)}`);
                    break;
                    
                case 'typing':
                    broadcastToChannel(data.serverId, data.channelId, {
                        type: 'typing',
                        serverId: data.serverId,
                        channelId: data.channelId,
                        user: currentUser
                    }, currentUser.id); // Exclude sender
                    break;
                    
                case 'status':
                    // Update user status
                    if (currentUser && data.status) {
                        currentUser.status = data.status;
                        sendOnlineUsers(currentServerId, currentChannelId);
                    }
                    break;
                    
                default:
                    console.log('Unknown message type:', data.type);
            }
        } catch (error) {
            console.error('Error processing message:', error);
        }
    });

    ws.on('close', () => {
        if (currentUser && currentServerId && currentChannelId) {
            // Remove user from connected users
            if (connectedUsers[currentServerId] && 
                connectedUsers[currentServerId][currentChannelId] && 
                connectedUsers[currentServerId][currentChannelId][currentUser.id]) {
                
                delete connectedUsers[currentServerId][currentChannelId][currentUser.id];
                
                // Send system message about user leaving
                const leaveMessage = {
                    id: Math.random().toString(36).substr(2, 9),
                    content: `${currentUser.username} has left the channel`,
                    author: { 
                        id: 'system', 
                        username: 'System', 
                        avatar: 'S' 
                    },
                    timestamp: new Date().toISOString(),
                    system: true
                };
                
                // Broadcast to all users in the channel
                broadcastToChannel(currentServerId, currentChannelId, {
                    type: 'message',
                    serverId: currentServerId,
                    channelId: currentChannelId,
                    message: leaveMessage
                });
                
                // Update online users list
                sendOnlineUsers(currentServerId, currentChannelId);
                
                console.log(`User ${currentUser.username} disconnected from ${currentServerId}/${currentChannelId}`);
            }
        }
    });
    
    // Send ping every 30 seconds to keep connection alive
    const pingInterval = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'ping' }));
        } else {
            clearInterval(pingInterval);
        }
    }, 30000);
});

function broadcastToChannel(serverId, channelId, data, excludeUserId = null) {
    if (!connectedUsers[serverId] || !connectedUsers[serverId][channelId]) {
        return;
    }
    
    const users = connectedUsers[serverId][channelId];
    const message = JSON.stringify(data);
    
    Object.keys(users).forEach(userId => {
        if (excludeUserId && userId === excludeUserId) {
            return; // Skip excluded user
        }
        
        const connection = users[userId];
        if (connection.ws.readyState === WebSocket.OPEN) {
            connection.ws.send(message);
        }
    });
}

function sendOnlineUsers(serverId, channelId) {
    if (!connectedUsers[serverId] || !connectedUsers[serverId][channelId]) {
        return;
    }
    
    const users = connectedUsers[serverId][channelId];
    const onlineUsers = Object.values(users).map(conn => conn.user);
    
    broadcastToChannel(serverId, channelId, {
        type: 'users',
        serverId: serverId,
        channelId: channelId,
        users: onlineUsers
    });
}

// Create some dummy data for testing
function createInitialData() {
    // Create some servers
    const servers = ['general', 'gaming', 'coding', 'design'];
    const channels = ['general', 'random', 'help'];
    
    servers.forEach(server => {
        if (!connectedUsers[server]) {
            connectedUsers[server] = {};
        }
        
        channels.forEach(channel => {
            if (!connectedUsers[server][channel]) {
                connectedUsers[server][channel] = {};
            }
            
            const historyKey = `${server}-${channel}`;
            if (!messageHistory[historyKey]) {
                messageHistory[historyKey] = [];
                
                // Add welcome message
                messageHistory[historyKey].push({
                    id: Math.random().toString(36).substr(2, 9),
                    content: `Welcome to #${channel} in ${server} server!`,
                    author: { 
                        id: 'system', 
                        username: 'System', 
                        avatar: 'S' 
                    },
                    timestamp: new Date().toISOString(),
                    system: true
                });
            }
        });
    });
}

// Initialize server data
createInitialData();

// Handle server errors
server.on('error', (err) => {
    console.error('Server error:', err);
});

// Start the server
server.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
    console.log(`WebSocket server is running on ws://localhost:${PORT}`);
});

// Handle process termination
process.on('SIGINT', () => {
    console.log('Server shutting down...');
    wss.clients.forEach(client => {
        client.close();
    });
    server.close(() => {
        console.log('Server closed');
        process.exit(0);
    });
});