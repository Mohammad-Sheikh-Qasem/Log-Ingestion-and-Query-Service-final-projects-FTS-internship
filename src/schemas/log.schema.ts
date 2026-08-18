import { z } from 'zod';


const isFlatObject = (val: Record<string, unknown>) => {
    return Object.values(val).every(
        (v) => typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean' || v === null
        //
    );
};

export const LogItemSchema = z.object({
    timestamp: z.string().datetime().refine((val) => {
        const logTime = new Date(val).getTime();
        const maxAllowedTime = Date.now() + 5 * 60 * 1000; //
        return logTime <= maxAllowedTime;
    }, { message: "Timestamp cannot be more than 5 minutes in the future" }),

    level: z.enum(['debug', 'info', 'warn', 'error']),
    service: z.string().trim().min(1, "Service cannot be empty"),
    message: z.string().trim().min(1, "Message cannot be empty"),

    attributes: z.record(z.string(),z.unknown()).optional().default({})
        .refine((val) => isFlatObject(val), {
            message: "Attributes must be a flat object without nested objects or arrays"
        })
});

export type LogItem = z.infer<typeof LogItemSchema>;