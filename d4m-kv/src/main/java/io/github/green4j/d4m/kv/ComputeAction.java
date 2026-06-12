/*
 * MIT License
 *
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package io.github.green4j.d4m.kv;

/**
 * An action invoked atomically against a single key by
 * {@link KeyValues#compute(io.github.green4j.d4m.common.AtomicBuffer, int, int, ComputeAction)}.
 *
 * <p>Implementations must:
 * <ul>
 *   <li>return the same non-null {@link ComputeContext} instance from
 *       {@link #context()} on every call (the {@link KeyValues}
 *       implementation relies on this for its zero-allocation contract);</li>
 *   <li>inside {@link #execute()}, touch the store <em>only</em> through the
 *       provided context - calling other {@link KeyValues} methods would
 *       acquire further locks in an undefined order;</li>
 *   <li>store any "result" of the action in their own fields, to be read by
 *       the caller after {@link KeyValues#compute} returns.</li>
 * </ul>
 */
public interface ComputeAction {
    /**
     * Returns the reusable {@link ComputeContext} that the {@link KeyValues}
     * implementation should bind to this call's key before invoking
     * {@link #execute()}.
     *
     * @return the context owned by this action
     */
    ComputeContext context();

    /**
     * Executed once per {@link KeyValues#compute} call, with the context
     * already bound to the key and any implementation-defined lock acquired.
     */
    void execute();
}
