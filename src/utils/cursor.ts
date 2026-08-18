export interface CursorPayload {
    timestamp: string;
    id: string;
}

export function encodeCursor(payload: CursorPayload): string {
    return Buffer.from(JSON.stringify(payload)).toString('base64');
}