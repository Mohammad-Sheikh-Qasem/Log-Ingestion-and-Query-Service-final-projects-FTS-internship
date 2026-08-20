"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.LogItemSchema = void 0;
const zod_1 = require("zod");
const isFlatObject = (val) => {
    return Object.values(val).every((v) => typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean' || v === null
    //
    );
};
exports.LogItemSchema = zod_1.z.object({
    timestamp: zod_1.z.string().datetime().refine((val) => {
        const logTime = new Date(val).getTime();
        const maxAllowedTime = Date.now() + 5 * 60 * 1000; //
        return logTime <= maxAllowedTime;
    }, { message: "Timestamp cannot be more than 5 minutes in the future" }),
    level: zod_1.z.enum(['debug', 'info', 'warn', 'error']),
    service: zod_1.z.string().trim().min(1, "Service cannot be empty"),
    message: zod_1.z.string().trim().min(1, "Message cannot be empty"),
    attributes: zod_1.z.record(zod_1.z.string(), zod_1.z.unknown()).optional().default({})
        .refine((val) => isFlatObject(val), {
        message: "Attributes must be a flat object without nested objects or arrays"
    })
});
