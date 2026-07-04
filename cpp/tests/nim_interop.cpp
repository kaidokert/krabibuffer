#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <vector>

#include "krabibuffer.hpp"

int main()
{
    static_assert(sizeof(std::uint64_t) == 8);
    constexpr std::uint64_t slot_count = 4;
    constexpr std::uint64_t stride = 4;
    constexpr std::size_t header_size = 40;
    std::vector<std::uint8_t> memory(header_size + slot_count * stride, 0);

    auto* kb = reinterpret_cast<KrabiBuffer*>(memory.data());
    kb->magic = 0xC3; // Nim PointerWidthMagic on 64-bit targets.
    kb->slot_count = slot_count;
    kb->stride = stride;
    kb->head = 0;
    kb->tail = 0;

    const std::array<std::uint8_t, stride> from_nim_style_memory{ 'n', 'i', 'm', '!' };
    assert(krabibuffer_enqueue(kb, from_nim_style_memory.data()));

    std::array<std::uint8_t, stride> read_by_cpp{};
    assert(krabibuffer_dequeue(kb, read_by_cpp.data()));
    assert(read_by_cpp == from_nim_style_memory);

    const std::array<std::uint8_t, stride> written_by_cpp{ 'c', '+', '+', '!' };
    assert(krabibuffer_enqueue(kb, written_by_cpp.data()));

    // Validate the raw ABI-visible bytes a Nim consumer would read.
    assert(kb->head == 1);
    assert(kb->tail == 2);
    assert(std::memcmp(memory.data() + header_size + stride, written_by_cpp.data(), stride) == 0);

    std::cout << "C++/Nim ABI interop passed\n";
    return 0;
}
