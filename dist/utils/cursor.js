"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.encodeCursor = encodeCursor;
exports.decodeCursor = decodeCursor;
function encodeCursor(payload) {
    return Buffer.from(JSON.stringify(payload)).toString('base64');
}
function decodeCursor(cursorStr) {
    try {
        const jsonStr = Buffer.from(cursorStr, 'base64').toString('utf-8');
        const parsed = JSON.parse(jsonStr);
        if (parsed.timestamp && parsed.id) {
            return parsed;
        }
        return null;
    }
    catch (error) {
        return null;
    }
}
