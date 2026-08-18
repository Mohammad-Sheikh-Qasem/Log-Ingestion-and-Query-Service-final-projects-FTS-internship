export interface CursorPayload {
    timestamp: string;
    id: string;
}

export function encodeCursor(payload: CursorPayload): string {
    return Buffer.from(JSON.stringify(payload)).toString('base64');
}

export function decodeCursor(cursorStr: string): CursorPayload | null {
    try {
        const jsonStr = Buffer.from(cursorStr, 'base64').toString('utf-8');
        const parsed = JSON.parse(jsonStr);

        if(parsed.timestamp && parsed.id){
            return parsed;
        }
        return null;
    }catch(error){
        return null;
    }
}