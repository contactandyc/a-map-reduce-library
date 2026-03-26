// SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-map-reduce-library/amr.h"
#include "a-map-reduce-library/amr_common_datatypes.h"
#include <string.h>

/* ========================================================================
 * HASHING & POINTER MATH HELPERS
 * ======================================================================== */

static inline size_t internal_hash_str(const char *s) {
    size_t h = 0;
    while (*s) h = h * 131 + (unsigned char)*s++;
    return h;
}

static inline size_t internal_hash_combine(size_t h1, size_t h2) {
    return h1 * 131 + h2;
}

static inline const char *sp_get_b(const char *r) {
    return r + strlen(r) + 1;
}

static inline const char *spw_get_a(const char *r) {
    return r + sizeof(double);
}
static inline const char *spw_get_b(const char *r) {
    const char *a = spw_get_a(r); return a + strlen(a) + 1;
}

static inline const char *spws_get_a(const char *r) {
    return r + (3 * sizeof(double));
}
static inline const char *spws_get_b(const char *r) {
    const char *a = spws_get_a(r); return a + strlen(a) + 1;
}

/* ========================================================================
 * 1. StringSingleton
 * ======================================================================== */
static void ss_ser(const void *o, aml_buffer_t *bh) {
    const amr_string_singleton_t *ss = (const amr_string_singleton_t *)o;
    aml_buffer_appends(bh, ss->str ? ss->str : "");
    aml_buffer_appendc(bh, '\0');
}
static void* ss_des(aml_pool_t *p, const void *b, size_t l) {
    (void)l;
    amr_string_singleton_t *ss = aml_pool_zalloc(p, sizeof(*ss));
    ss->str = aml_pool_strdup(p, (const char *)b);
    return ss;
}
static void ss_str(const void *o, aml_buffer_t *bh) {
    aml_buffer_appendf(bh, "Str: %s", ((const amr_string_singleton_t *)o)->str);
}
static size_t ss_part(const io_record_t *r, size_t num_part, void *arg) {
    return internal_hash_str((const char *)r->record) % num_part;
}
static int ss_cmp(const io_record_t *a, const io_record_t *b, void *arg) {
    return strcmp((const char *)a->record, (const char *)b->record);
}

/* ========================================================================
 * 2. StringPair
 * ======================================================================== */
static void sp_ser(const void *o, aml_buffer_t *bh) {
    const amr_string_pair_t *sp = (const amr_string_pair_t *)o;
    aml_buffer_appends(bh, sp->a ? sp->a : "");
    aml_buffer_appendc(bh, '\0');
    aml_buffer_appends(bh, sp->b ? sp->b : "");
    aml_buffer_appendc(bh, '\0');
}
static void* sp_des(aml_pool_t *p, const void *b, size_t l) {
    (void)l;
    amr_string_pair_t *sp = aml_pool_zalloc(p, sizeof(*sp));
    const char *ptr = (const char *)b;
    sp->a = aml_pool_strdup(p, ptr);
    sp->b = aml_pool_strdup(p, sp_get_b(ptr));
    return sp;
}
static void sp_str(const void *o, aml_buffer_t *bh) {
    const amr_string_pair_t *sp = (const amr_string_pair_t *)o;
    aml_buffer_appendf(bh, "A: %s | B: %s", sp->a, sp->b);
}

/* StringPair Partitions */
static size_t sp_part_a(const io_record_t *r, size_t np, void *arg) {
    return internal_hash_str((const char *)r->record) % np;
}
static size_t sp_part_b(const io_record_t *r, size_t np, void *arg) {
    return internal_hash_str(sp_get_b((const char *)r->record)) % np;
}
static size_t sp_part_ab(const io_record_t *r, size_t np, void *arg) {
    size_t ha = internal_hash_str((const char *)r->record);
    size_t hb = internal_hash_str(sp_get_b((const char *)r->record));
    return internal_hash_combine(ha, hb) % np;
}
static size_t sp_part_ba(const io_record_t *r, size_t np, void *arg) {
    size_t ha = internal_hash_str((const char *)r->record);
    size_t hb = internal_hash_str(sp_get_b((const char *)r->record));
    return internal_hash_combine(hb, ha) % np;
}

/* StringPair Comparators */
static int sp_cmp_a(const io_record_t *a, const io_record_t *b, void *arg) {
    return strcmp((const char *)a->record, (const char *)b->record);
}
static int sp_cmp_b(const io_record_t *a, const io_record_t *b, void *arg) {
    return strcmp(sp_get_b((const char *)a->record), sp_get_b((const char *)b->record));
}
static int sp_cmp_ab(const io_record_t *a, const io_record_t *b, void *arg) {
    int c = strcmp((const char *)a->record, (const char *)b->record);
    if (c != 0) return c;
    return strcmp(sp_get_b((const char *)a->record), sp_get_b((const char *)b->record));
}
static int sp_cmp_ba(const io_record_t *a, const io_record_t *b, void *arg) {
    int c = strcmp(sp_get_b((const char *)a->record), sp_get_b((const char *)b->record));
    if (c != 0) return c;
    return strcmp((const char *)a->record, (const char *)b->record);
}

/* ========================================================================
 * 3. StringWeight
 * ======================================================================== */
static void sw_ser(const void *o, aml_buffer_t *bh) {
    const amr_string_weight_t *sw = (const amr_string_weight_t *)o;
    aml_buffer_append(bh, &sw->w, sizeof(double));
    aml_buffer_appends(bh, sw->str ? sw->str : "");
    aml_buffer_appendc(bh, '\0');
}
static void* sw_des(aml_pool_t *p, const void *b, size_t l) {
    (void)l;
    amr_string_weight_t *sw = aml_pool_zalloc(p, sizeof(*sw));
    const char *ptr = (const char *)b;
    memcpy(&sw->w, ptr, sizeof(double));
    sw->str = aml_pool_strdup(p, ptr + sizeof(double));
    return sw;
}
static void sw_str(const void *o, aml_buffer_t *bh) {
    const amr_string_weight_t *sw = (const amr_string_weight_t *)o;
    aml_buffer_appendf(bh, "W: %.6f | Str: %s", sw->w, sw->str);
}
static size_t sw_part(const io_record_t *r, size_t np, void *arg) {
    return internal_hash_str((const char *)(r->record + sizeof(double))) % np;
}
static int sw_cmp_str(const io_record_t *a, const io_record_t *b, void *arg) {
    return strcmp((const char *)(a->record + sizeof(double)),
                  (const char *)(b->record + sizeof(double)));
}

/* STABLE Sort_W_Desc */
static int sw_cmp_w_desc(const io_record_t *a, const io_record_t *b, void *arg) {
    double wa, wb;
    memcpy(&wa, a->record, sizeof(double));
    memcpy(&wb, b->record, sizeof(double));
    if (wa > wb) return -1;
    if (wa < wb) return 1;
    // Tie-breaker: sort by string
    return strcmp((const char *)(a->record + sizeof(double)),
                  (const char *)(b->record + sizeof(double)));
}

/* ========================================================================
 * 4. StringPairWeight
 * ======================================================================== */
static void spw_ser(const void *o, aml_buffer_t *bh) {
    const amr_string_pair_weight_t *spw = (const amr_string_pair_weight_t *)o;
    aml_buffer_append(bh, &spw->w, sizeof(double));
    aml_buffer_appends(bh, spw->a ? spw->a : "");
    aml_buffer_appendc(bh, '\0');
    aml_buffer_appends(bh, spw->b ? spw->b : "");
    aml_buffer_appendc(bh, '\0');
}
static void* spw_des(aml_pool_t *p, const void *b, size_t l) {
    (void)l;
    amr_string_pair_weight_t *spw = aml_pool_zalloc(p, sizeof(*spw));
    const char *ptr = (const char *)b;
    memcpy(&spw->w, ptr, sizeof(double));
    spw->a = aml_pool_strdup(p, spw_get_a(ptr));
    spw->b = aml_pool_strdup(p, spw_get_b(ptr));
    return spw;
}
static void spw_str(const void *o, aml_buffer_t *bh) {
    const amr_string_pair_weight_t *spw = (const amr_string_pair_weight_t *)o;
    aml_buffer_appendf(bh, "W: %.6f | %s -> %s", spw->w, spw->a, spw->b);
}

/* StringPairWeight Partitions */
static size_t spw_part_a(const io_record_t *r, size_t np, void *arg) {
    return internal_hash_str(spw_get_a((const char *)r->record)) % np;
}
static size_t spw_part_b(const io_record_t *r, size_t np, void *arg) {
    return internal_hash_str(spw_get_b((const char *)r->record)) % np;
}
static size_t spw_part_ab(const io_record_t *r, size_t np, void *arg) {
    size_t ha = internal_hash_str(spw_get_a((const char *)r->record));
    size_t hb = internal_hash_str(spw_get_b((const char *)r->record));
    return internal_hash_combine(ha, hb) % np;
}
static size_t spw_part_ba(const io_record_t *r, size_t np, void *arg) {
    size_t ha = internal_hash_str(spw_get_a((const char *)r->record));
    size_t hb = internal_hash_str(spw_get_b((const char *)r->record));
    return internal_hash_combine(hb, ha) % np;
}

/* StringPairWeight Comparators */
static int spw_cmp_a(const io_record_t *a, const io_record_t *b, void *arg) {
    return strcmp(spw_get_a((const char *)a->record), spw_get_a((const char *)b->record));
}
static int spw_cmp_b(const io_record_t *a, const io_record_t *b, void *arg) {
    return strcmp(spw_get_b((const char *)a->record), spw_get_b((const char *)b->record));
}
static int spw_cmp_ab(const io_record_t *a, const io_record_t *b, void *arg) {
    int c = strcmp(spw_get_a((const char *)a->record), spw_get_a((const char *)b->record));
    if (c != 0) return c;
    return strcmp(spw_get_b((const char *)a->record), spw_get_b((const char *)b->record));
}
static int spw_cmp_ba(const io_record_t *a, const io_record_t *b, void *arg) {
    int c = strcmp(spw_get_b((const char *)a->record), spw_get_b((const char *)b->record));
    if (c != 0) return c;
    return strcmp(spw_get_a((const char *)a->record), spw_get_a((const char *)b->record));
}

/* STABLE Sort_W_Desc */
static int spw_cmp_w_desc(const io_record_t *a, const io_record_t *b, void *arg) {
    double wa, wb;
    memcpy(&wa, a->record, sizeof(double));
    memcpy(&wb, b->record, sizeof(double));
    if (wa > wb) return -1;
    if (wa < wb) return 1;
    // Tie-breaker: A then B
    int c = strcmp(spw_get_a((const char *)a->record), spw_get_a((const char *)b->record));
    if (c != 0) return c;
    return strcmp(spw_get_b((const char *)a->record), spw_get_b((const char *)b->record));
}

/* ========================================================================
 * 5. StringPairWeights (Plural)
 * ======================================================================== */
static void spws_ser(const void *o, aml_buffer_t *bh) {
    const amr_string_pair_weights_t *spws = (const amr_string_pair_weights_t *)o;
    aml_buffer_append(bh, &spws->w,  sizeof(double));
    aml_buffer_append(bh, &spws->aw, sizeof(double));
    aml_buffer_append(bh, &spws->bw, sizeof(double));
    aml_buffer_appends(bh, spws->a ? spws->a : "");
    aml_buffer_appendc(bh, '\0');
    aml_buffer_appends(bh, spws->b ? spws->b : "");
    aml_buffer_appendc(bh, '\0');
}
static void* spws_des(aml_pool_t *p, const void *b, size_t l) {
    (void)l;
    amr_string_pair_weights_t *spws = aml_pool_zalloc(p, sizeof(*spws));
    const char *ptr = (const char *)b;
    memcpy(&spws->w,  ptr, sizeof(double)); ptr += sizeof(double);
    memcpy(&spws->aw, ptr, sizeof(double)); ptr += sizeof(double);
    memcpy(&spws->bw, ptr, sizeof(double));

    spws->a = aml_pool_strdup(p, spws_get_a((const char *)b));
    spws->b = aml_pool_strdup(p, spws_get_b((const char *)b));
    return spws;
}
static void spws_str(const void *o, aml_buffer_t *bh) {
    const amr_string_pair_weights_t *s = (const amr_string_pair_weights_t *)o;
    aml_buffer_appendf(bh, "W: %.4f (A:%.1f B:%.1f) | %s -> %s", s->w, s->aw, s->bw, s->a, s->b);
}

/* StringPairWeights Partitions */
static size_t spws_part_a(const io_record_t *r, size_t np, void *arg) {
    return internal_hash_str(spws_get_a((const char *)r->record)) % np;
}
static size_t spws_part_b(const io_record_t *r, size_t np, void *arg) {
    return internal_hash_str(spws_get_b((const char *)r->record)) % np;
}
static size_t spws_part_ab(const io_record_t *r, size_t np, void *arg) {
    size_t ha = internal_hash_str(spws_get_a((const char *)r->record));
    size_t hb = internal_hash_str(spws_get_b((const char *)r->record));
    return internal_hash_combine(ha, hb) % np;
}
static size_t spws_part_ba(const io_record_t *r, size_t np, void *arg) {
    size_t ha = internal_hash_str(spws_get_a((const char *)r->record));
    size_t hb = internal_hash_str(spws_get_b((const char *)r->record));
    return internal_hash_combine(hb, ha) % np;
}

/* StringPairWeights Comparators */
static int spws_cmp_a(const io_record_t *a, const io_record_t *b, void *arg) {
    return strcmp(spws_get_a((const char *)a->record), spws_get_a((const char *)b->record));
}
static int spws_cmp_b(const io_record_t *a, const io_record_t *b, void *arg) {
    return strcmp(spws_get_b((const char *)a->record), spws_get_b((const char *)b->record));
}
static int spws_cmp_ab(const io_record_t *a, const io_record_t *b, void *arg) {
    int c = strcmp(spws_get_a((const char *)a->record), spws_get_a((const char *)b->record));
    if (c != 0) return c;
    return strcmp(spws_get_b((const char *)a->record), spws_get_b((const char *)b->record));
}
static int spws_cmp_ba(const io_record_t *a, const io_record_t *b, void *arg) {
    int c = strcmp(spws_get_b((const char *)a->record), spws_get_b((const char *)b->record));
    if (c != 0) return c;
    return strcmp(spws_get_a((const char *)a->record), spws_get_a((const char *)b->record));
}

/* STABLE Sort_W_Desc */
static int spws_cmp_w_desc(const io_record_t *a, const io_record_t *b, void *arg) {
    double wa, wb;
    memcpy(&wa, a->record, sizeof(double));
    memcpy(&wb, b->record, sizeof(double));
    if (wa > wb) return -1;
    if (wa < wb) return 1;
    // Tie-breaker: A then B
    int c = strcmp(spws_get_a((const char *)a->record), spws_get_a((const char *)b->record));
    if (c != 0) return c;
    return strcmp(spws_get_b((const char *)a->record), spws_get_b((const char *)b->record));
}

/* ========================================================================
 * 6. UInt32Singleton
 * ======================================================================== */
static void uint32_singleton_ser(const void *o, aml_buffer_t *bh) {
    aml_buffer_append(bh, &((const amr_uint32_singleton_t *)o)->val, sizeof(uint32_t));
}
static void* uint32_singleton_des(aml_pool_t *p, const void *b, size_t l) {
    (void)l;
    amr_uint32_singleton_t *v = aml_pool_alloc(p, sizeof(*v));
    memcpy(&v->val, b, sizeof(uint32_t));
    return v;
}
static void uint32_singleton_str(const void *o, aml_buffer_t *bh) {
    aml_buffer_appendf(bh, "UInt32(%u)", ((const amr_uint32_singleton_t *)o)->val);
}

/* ========================================================================
 * 7. UInt32Pair
 * ======================================================================== */
static void uint32_pair_ser(const void *o, aml_buffer_t *bh) {
    aml_buffer_append(bh, o, 2 * sizeof(uint32_t));
}
static void* uint32_pair_des(aml_pool_t *p, const void *b, size_t l) {
    (void)l;
    amr_uint32_pair_t *v = aml_pool_alloc(p, sizeof(*v));
    memcpy(v, b, 2 * sizeof(uint32_t));
    return v;
}
static void uint32_pair_str(const void *o, aml_buffer_t *bh) {
    const amr_uint32_pair_t *v = (const amr_uint32_pair_t *)o;
    aml_buffer_appendf(bh, "UInt32Pair(%u, %u)", v->a, v->b);
}

static size_t uint32_pair_part_a(const io_record_t *r, size_t np, void *arg) {
    uint32_t a;
    memcpy(&a, r->record, sizeof(uint32_t));
    return a % np;
}

static size_t uint32_pair_part_b(const io_record_t *r, size_t np, void *arg) {
    const uint32_t *p = (const uint32_t *)r->record;
    return p[1] % np;
}

static int uint32_pair_cmp_a(const io_record_t *rA, const io_record_t *rB, void *arg) {
    uint32_t a1, a2;
    memcpy(&a1, rA->record, sizeof(uint32_t));
    memcpy(&a2, rB->record, sizeof(uint32_t));
    return (a1 > a2) ? 1 : (a1 < a2 ? -1 : 0);
}

static int uint32_pair_cmp_a_b(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const uint32_t *p1 = (const uint32_t *)rA->record;
    const uint32_t *p2 = (const uint32_t *)rB->record;
    if (p1[0] != p2[0]) return (p1[0] > p2[0]) ? 1 : -1;
    if (p1[1] != p2[1]) return (p1[1] > p2[1]) ? 1 : -1;
    return 0;
}

static int uint32_pair_cmp_b_a(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const uint32_t *p1 = (const uint32_t *)rA->record;
    const uint32_t *p2 = (const uint32_t *)rB->record;
    if (p1[1] != p2[1]) return (p1[1] > p2[1]) ? 1 : -1;
    if (p1[0] != p2[0]) return (p1[0] > p2[0]) ? 1 : -1;
    return 0;
}

/* ========================================================================
 * 8. IdStringPair
 * ======================================================================== */
static void id_string_pair_ser(const void *o, aml_buffer_t *bh) {
    const amr_id_string_pair_t *v = (const amr_id_string_pair_t *)o;
    aml_buffer_append(bh, &v->id, sizeof(uint32_t));
    aml_buffer_appends(bh, v->str ? v->str : "");
    aml_buffer_appendc(bh, '\0');
}
static void* id_string_pair_des(aml_pool_t *p, const void *b, size_t l) {
    (void)l;
    amr_id_string_pair_t *v = aml_pool_alloc(p, sizeof(*v));
    memcpy(&v->id, b, sizeof(uint32_t));
    v->str = aml_pool_strdup(p, (const char *)b + sizeof(uint32_t));
    return v;
}
static void id_string_pair_str(const void *o, aml_buffer_t *bh) {
    const amr_id_string_pair_t *v = (const amr_id_string_pair_t *)o;
    aml_buffer_appendf(bh, "IdString(%u, %s)", v->id, v->str);
}
static size_t id_string_pair_part_str(const io_record_t *r, size_t np, void *arg) {
    const char *str = (const char *)r->record + sizeof(uint32_t);
    return internal_hash_str(str) % np;
}
static int id_string_pair_cmp_str(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const char *s1 = (const char *)rA->record + sizeof(uint32_t);
    const char *s2 = (const char *)rB->record + sizeof(uint32_t);
    return strcmp(s1, s2);
}

/* ========================================================================
 * 9. IDPairWeight (Single Weight)
 * ======================================================================== */
static void idpw_ser(const void *o, aml_buffer_t *bh) {
    aml_buffer_append(bh, o, sizeof(amr_id_pair_weight_t));
}

static void* idpw_des(aml_pool_t *p, const void *b, size_t l) {
    (void)p; (void)l;
    return (void *)b;
}

static void idpw_str(const void *o, aml_buffer_t *bh) {
    const amr_id_pair_weight_t *v = (const amr_id_pair_weight_t *)o;
    aml_buffer_appendf(bh, "W: %.4f | %u -> %u", v->w, v->a, v->b);
}

/* --- Partitioners --- */
static size_t idpw_part_a(const io_record_t *r, size_t np, void *arg) {
    return ((const amr_id_pair_weight_t *)r->record)->a % np;
}

static size_t idpw_part_b(const io_record_t *r, size_t np, void *arg) {
    return ((const amr_id_pair_weight_t *)r->record)->b % np;
}

/* --- Comparators --- */
static int idpw_cmp_a(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const amr_id_pair_weight_t *pa = (const amr_id_pair_weight_t *)rA->record;
    const amr_id_pair_weight_t *pb = (const amr_id_pair_weight_t *)rB->record;
    return (pa->a > pb->a) ? 1 : (pa->a < pb->a ? -1 : 0);
}

static int idpw_cmp_b(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const amr_id_pair_weight_t *pa = (const amr_id_pair_weight_t *)rA->record;
    const amr_id_pair_weight_t *pb = (const amr_id_pair_weight_t *)rB->record;
    return (pa->b > pb->b) ? 1 : (pa->b < pb->b ? -1 : 0);
}

static int idpw_cmp_ab(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const amr_id_pair_weight_t *pa = (const amr_id_pair_weight_t *)rA->record;
    const amr_id_pair_weight_t *pb = (const amr_id_pair_weight_t *)rB->record;
    if (pa->a != pb->a) return (pa->a > pb->a) ? 1 : -1;
    if (pa->b != pb->b) return (pa->b > pb->b) ? 1 : -1;
    return 0;
}

static int idpw_cmp_ba(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const amr_id_pair_weight_t *pa = (const amr_id_pair_weight_t *)rA->record;
    const amr_id_pair_weight_t *pb = (const amr_id_pair_weight_t *)rB->record;
    if (pa->b != pb->b) return (pa->b > pb->b) ? 1 : -1;
    if (pa->a != pb->a) return (pa->a > pb->a) ? 1 : -1;
    return 0;
}

static int idpw_cmp_a_w(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const amr_id_pair_weight_t *pa = (const amr_id_pair_weight_t *)rA->record;
    const amr_id_pair_weight_t *pb = (const amr_id_pair_weight_t *)rB->record;
    if (pa->a != pb->a) return (pa->a > pb->a) ? 1 : -1;
    if (pa->w != pb->w) return (pa->w > pb->w) ? 1 : -1; // W Ascending
    if (pa->b != pb->b) return (pa->b > pb->b) ? 1 : -1; // Tie-break B
    return 0;
}

static int idpw_cmp_a_wdesc(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const amr_id_pair_weight_t *pa = (const amr_id_pair_weight_t *)rA->record;
    const amr_id_pair_weight_t *pb = (const amr_id_pair_weight_t *)rB->record;
    if (pa->a != pb->a) return (pa->a > pb->a) ? 1 : -1;
    if (pa->w != pb->w) return (pa->w > pb->w) ? -1 : 1; // W Descending
    if (pa->b != pb->b) return (pa->b > pb->b) ? 1 : -1; // Tie-break B
    return 0;
}

static int idpw_cmp_b_w(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const amr_id_pair_weight_t *pa = (const amr_id_pair_weight_t *)rA->record;
    const amr_id_pair_weight_t *pb = (const amr_id_pair_weight_t *)rB->record;
    if (pa->b != pb->b) return (pa->b > pb->b) ? 1 : -1;
    if (pa->w != pb->w) return (pa->w > pb->w) ? 1 : -1; // W Ascending
    if (pa->a != pb->a) return (pa->a > pb->a) ? 1 : -1; // Tie-break A
    return 0;
}

static int idpw_cmp_b_wdesc(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const amr_id_pair_weight_t *pa = (const amr_id_pair_weight_t *)rA->record;
    const amr_id_pair_weight_t *pb = (const amr_id_pair_weight_t *)rB->record;
    if (pa->b != pb->b) return (pa->b > pb->b) ? 1 : -1;
    if (pa->w != pb->w) return (pa->w > pb->w) ? -1 : 1; // W Descending
    if (pa->a != pb->a) return (pa->a > pb->a) ? 1 : -1; // Tie-break A
    return 0;
}

/* ========================================================================
 * 10. IDPairWeights (Dense Integer Math Struct)
 * ======================================================================== */
static void idpws_ser(const void *o, aml_buffer_t *bh) {
    aml_buffer_append(bh, o, sizeof(amr_id_pair_weights_t));
}

// Zero-copy deserialization: just cast the raw buffer!
static void* idpws_des(aml_pool_t *p, const void *b, size_t l) {
    (void)p; (void)l;
    return (void *)b;
}

static void idpws_str(const void *o, aml_buffer_t *bh) {
    const amr_id_pair_weights_t *v = (const amr_id_pair_weights_t *)o;
    aml_buffer_appendf(bh, "W: %.4f (A:%.1f B:%.1f) | %u -> %u", v->w, v->aw, v->bw, v->a, v->b);
}

/* --- Partitioners --- */
static size_t idpws_part_a(const io_record_t *r, size_t np, void *arg) {
    return ((const amr_id_pair_weights_t *)r->record)->a % np;
}

static size_t idpws_part_b(const io_record_t *r, size_t np, void *arg) {
    return ((const amr_id_pair_weights_t *)r->record)->b % np;
}

/* --- Comparators (All use the unlisted entity as a final ascending tie-breaker) --- */
static int idpws_cmp_a(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const amr_id_pair_weights_t *pa = (const amr_id_pair_weights_t *)rA->record;
    const amr_id_pair_weights_t *pb = (const amr_id_pair_weights_t *)rB->record;
    return (pa->a > pb->a) ? 1 : (pa->a < pb->a ? -1 : 0);
}

static int idpws_cmp_b(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const amr_id_pair_weights_t *pa = (const amr_id_pair_weights_t *)rA->record;
    const amr_id_pair_weights_t *pb = (const amr_id_pair_weights_t *)rB->record;
    return (pa->b > pb->b) ? 1 : (pa->b < pb->b ? -1 : 0);
}

static int idpws_cmp_ab(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const amr_id_pair_weights_t *pa = (const amr_id_pair_weights_t *)rA->record;
    const amr_id_pair_weights_t *pb = (const amr_id_pair_weights_t *)rB->record;
    if (pa->a != pb->a) return (pa->a > pb->a) ? 1 : -1;
    if (pa->b != pb->b) return (pa->b > pb->b) ? 1 : -1;
    return 0;
}

static int idpws_cmp_ba(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const amr_id_pair_weights_t *pa = (const amr_id_pair_weights_t *)rA->record;
    const amr_id_pair_weights_t *pb = (const amr_id_pair_weights_t *)rB->record;
    if (pa->b != pb->b) return (pa->b > pb->b) ? 1 : -1;
    if (pa->a != pb->a) return (pa->a > pb->a) ? 1 : -1;
    return 0;
}

static int idpws_cmp_a_w(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const amr_id_pair_weights_t *pa = (const amr_id_pair_weights_t *)rA->record;
    const amr_id_pair_weights_t *pb = (const amr_id_pair_weights_t *)rB->record;
    if (pa->a != pb->a) return (pa->a > pb->a) ? 1 : -1;
    if (pa->w != pb->w) return (pa->w > pb->w) ? 1 : -1; // W Ascending
    if (pa->bw != pb->bw) return (pa->bw > pb->bw) ? -1 : 1; // Tie-break B weight descending
    if (pa->b != pb->b) return (pa->b > pb->b) ? 1 : -1; // Tie-break B
    return 0;
}

static int idpws_cmp_a_wdesc(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const amr_id_pair_weights_t *pa = (const amr_id_pair_weights_t *)rA->record;
    const amr_id_pair_weights_t *pb = (const amr_id_pair_weights_t *)rB->record;
    if (pa->a != pb->a) return (pa->a > pb->a) ? 1 : -1;
    if (pa->w != pb->w) return (pa->w > pb->w) ? -1 : 1; // W Descending
    if (pa->bw != pb->bw) return (pa->bw > pb->bw) ? 1 : -1; // Tie-break B weight
    if (pa->b != pb->b) return (pa->b > pb->b) ? 1 : -1; // Tie-break B
    return 0;
}

static int idpws_cmp_b_w(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const amr_id_pair_weights_t *pa = (const amr_id_pair_weights_t *)rA->record;
    const amr_id_pair_weights_t *pb = (const amr_id_pair_weights_t *)rB->record;
    if (pa->b != pb->b) return (pa->b > pb->b) ? 1 : -1;
    if (pa->w != pb->w) return (pa->w > pb->w) ? 1 : -1; // W Ascending
    if (pa->aw != pb->aw) return (pa->aw < pb->aw) ? 1 : -1; // Tie-break A weight descending
    if (pa->a != pb->a) return (pa->a > pb->a) ? 1 : -1; // Tie-break A
    return 0;
}

static int idpws_cmp_b_wdesc(const io_record_t *rA, const io_record_t *rB, void *arg) {
    const amr_id_pair_weights_t *pa = (const amr_id_pair_weights_t *)rA->record;
    const amr_id_pair_weights_t *pb = (const amr_id_pair_weights_t *)rB->record;
    if (pa->b != pb->b) return (pa->b > pb->b) ? 1 : -1;
    if (pa->w != pb->w) return (pa->w > pb->w) ? -1 : 1; // W Descending
    if (pa->aw != pb->aw) return (pa->aw > pb->aw) ? 1 : -1; // Tie-break A weight
    if (pa->a != pb->a) return (pa->a > pb->a) ? 1 : -1; // Tie-break A
    return 0;
}

/* ========================================================================
 * SHARED REDUCERS
 * ======================================================================== */

/* A generic reducer for any type that starts with a `double w` and is followed
   by an arbitrary length byte payload (e.g. StringWeight, StringPairWeight) */
static bool common_sum_w_prefix_reducer(io_record_t *res, const io_record_t *r, size_t num_r, aml_buffer_t *bh, void *arg __attribute__((unused))) {
    double total = 0.0;
    for (size_t i = 0; i < num_r; i++) {
        double w; memcpy(&w, r[i].record, sizeof(double));
        total += w;
    }
    aml_buffer_clear(bh);
    aml_buffer_append(bh, &total, sizeof(double));
    size_t payload_len = r[0].length - sizeof(double);
    aml_buffer_append(bh, r[0].record + sizeof(double), payload_len);
    res->record = aml_buffer_data(bh);
    res->length = aml_buffer_length(bh);
    return true;
}

/* Reducer for IDPairWeight (where the double w is inside a fixed struct) */
static bool idpw_sum_w_reducer(io_record_t *res, const io_record_t *r, size_t num_r, aml_buffer_t *bh, void *arg __attribute__((unused))) {
    amr_id_pair_weight_t out;
    memcpy(&out, r[0].record, sizeof(amr_id_pair_weight_t));
    out.w = 0.0;
    for (size_t i = 0; i < num_r; i++) {
        amr_id_pair_weight_t tmp;
        memcpy(&tmp, r[i].record, sizeof(amr_id_pair_weight_t));
        out.w += tmp.w;
    }
    aml_buffer_clear(bh);
    aml_buffer_append(bh, &out, sizeof(amr_id_pair_weight_t));
    res->record = aml_buffer_data(bh);
    res->length = aml_buffer_length(bh);
    return true;
}


/* ========================================================================
 * MASTER REGISTRATION HOOK
 * ======================================================================== */
void amr_register_common_datatypes(amr_t *sched) {
    static bool registered = false;
    if (registered) return;
    registered = true;

    /* 1. StringSingleton */
    amr_register_datatype(sched, "StringSingleton", "A single string", ss_ser, ss_des, ss_str);
    amr_datatype_add_partition(sched, "StringSingleton", "Hash_Str", ss_part);
    amr_datatype_add_compare(sched, "StringSingleton", "Sort_Str", ss_cmp);

    /* 2. StringPair */
    amr_register_datatype(sched, "StringPair", "Two strings (A, B)", sp_ser, sp_des, sp_str);
    amr_datatype_add_partition(sched, "StringPair", "Hash_A", sp_part_a);
    amr_datatype_add_partition(sched, "StringPair", "Hash_B", sp_part_b);
    amr_datatype_add_partition(sched, "StringPair", "Hash_A_B", sp_part_ab);
    amr_datatype_add_partition(sched, "StringPair", "Hash_B_A", sp_part_ba);
    amr_datatype_add_compare(sched, "StringPair", "Sort_A", sp_cmp_a);
    amr_datatype_add_compare(sched, "StringPair", "Sort_B", sp_cmp_b);
    amr_datatype_add_compare(sched, "StringPair", "Sort_A_B", sp_cmp_ab);
    amr_datatype_add_compare(sched, "StringPair", "Sort_B_A", sp_cmp_ba);

    /* 3. StringWeight */
    amr_register_datatype(sched, "StringWeight", "Double followed by a string", sw_ser, sw_des, sw_str);
    amr_datatype_add_partition(sched, "StringWeight", "Hash_Str", sw_part);
    amr_datatype_add_compare(sched, "StringWeight", "Sort_Str", sw_cmp_str);
    amr_datatype_add_compare(sched, "StringWeight", "Sort_W_Desc", sw_cmp_w_desc);
    amr_datatype_add_reducer(sched, "StringWeight", "Sum_W", common_sum_w_prefix_reducer);

    /* 4. StringPairWeight */
    amr_register_datatype(sched, "StringPairWeight", "W, A, B", spw_ser, spw_des, spw_str);
    amr_datatype_add_partition(sched, "StringPairWeight", "Hash_A", spw_part_a);
    amr_datatype_add_partition(sched, "StringPairWeight", "Hash_B", spw_part_b);
    amr_datatype_add_partition(sched, "StringPairWeight", "Hash_A_B", spw_part_ab);
    amr_datatype_add_partition(sched, "StringPairWeight", "Hash_B_A", spw_part_ba);
    amr_datatype_add_compare(sched, "StringPairWeight", "Sort_A", spw_cmp_a);
    amr_datatype_add_compare(sched, "StringPairWeight", "Sort_B", spw_cmp_b);
    amr_datatype_add_compare(sched, "StringPairWeight", "Sort_A_B", spw_cmp_ab);
    amr_datatype_add_compare(sched, "StringPairWeight", "Sort_B_A", spw_cmp_ba);
    amr_datatype_add_compare(sched, "StringPairWeight", "Sort_W_Desc", spw_cmp_w_desc);
    amr_datatype_add_reducer(sched, "StringPairWeight", "Sum_W", common_sum_w_prefix_reducer);

    /* 5. StringPairWeights (Plural) */
    amr_register_datatype(sched, "StringPairWeights", "W, AW, BW, A, B", spws_ser, spws_des, spws_str);
    amr_datatype_add_partition(sched, "StringPairWeights", "Hash_A", spws_part_a);
    amr_datatype_add_partition(sched, "StringPairWeights", "Hash_B", spws_part_b);
    amr_datatype_add_partition(sched, "StringPairWeights", "Hash_A_B", spws_part_ab);
    amr_datatype_add_partition(sched, "StringPairWeights", "Hash_B_A", spws_part_ba);
    amr_datatype_add_compare(sched, "StringPairWeights", "Sort_A", spws_cmp_a);
    amr_datatype_add_compare(sched, "StringPairWeights", "Sort_B", spws_cmp_b);
    amr_datatype_add_compare(sched, "StringPairWeights", "Sort_A_B", spws_cmp_ab);
    amr_datatype_add_compare(sched, "StringPairWeights", "Sort_B_A", spws_cmp_ba);
    amr_datatype_add_compare(sched, "StringPairWeights", "Sort_W_Desc", spws_cmp_w_desc);

    /* 6. UInt32Singleton */
    amr_register_datatype(sched, "UInt32", "Single 32-bit integer",
                          uint32_singleton_ser, uint32_singleton_des, uint32_singleton_str);

    /* 7. UInt32Pair */
    amr_register_datatype(sched, "UInt32Pair", "Pair of 32-bit integers",
                          uint32_pair_ser, uint32_pair_des, uint32_pair_str);
    amr_datatype_add_partition(sched, "UInt32Pair", "Hash_A", uint32_pair_part_a);
    amr_datatype_add_partition(sched, "UInt32Pair", "Hash_B", uint32_pair_part_b);

    amr_datatype_add_compare(sched, "UInt32Pair", "Sort_A", uint32_pair_cmp_a);
    amr_datatype_add_compare(sched, "UInt32Pair", "Sort_A_B", uint32_pair_cmp_a_b);
    amr_datatype_add_compare(sched, "UInt32Pair", "Sort_B_A", uint32_pair_cmp_b_a);

    /* 8. IdStringPair */
    amr_register_datatype(sched, "IdStringPair", "UInt32 ID and a String",
                          id_string_pair_ser, id_string_pair_des, id_string_pair_str);
    amr_datatype_add_partition(sched, "IdStringPair", "Hash_Str", id_string_pair_part_str);
    amr_datatype_add_compare(sched, "IdStringPair", "Sort_Str", id_string_pair_cmp_str);

    /* 9. IDPairWeight (Single Weight) */
    amr_register_datatype(sched, "IDPairWeight", "W, A, B (uint32)",
                          idpw_ser, idpw_des, idpw_str);

    amr_datatype_add_partition(sched, "IDPairWeight", "Hash_A", idpw_part_a);
    amr_datatype_add_partition(sched, "IDPairWeight", "Hash_B", idpw_part_b);

    amr_datatype_add_compare(sched, "IDPairWeight", "Sort_A", idpw_cmp_a);
    amr_datatype_add_compare(sched, "IDPairWeight", "Sort_B", idpw_cmp_b);
    amr_datatype_add_compare(sched, "IDPairWeight", "Sort_A_B", idpw_cmp_ab);
    amr_datatype_add_compare(sched, "IDPairWeight", "Sort_B_A", idpw_cmp_ba);
    amr_datatype_add_compare(sched, "IDPairWeight", "Sort_A_W", idpw_cmp_a_w);
    amr_datatype_add_compare(sched, "IDPairWeight", "Sort_A_WDesc", idpw_cmp_a_wdesc);
    amr_datatype_add_compare(sched, "IDPairWeight", "Sort_B_W", idpw_cmp_b_w);
    amr_datatype_add_compare(sched, "IDPairWeight", "Sort_B_WDesc", idpw_cmp_b_wdesc);
    amr_datatype_add_reducer(sched, "IDPairWeight", "Sum_W", idpw_sum_w_reducer);

    /* 10. IDPairWeights */
    amr_register_datatype(sched, "IDPairWeights", "W, AW, BW, A, B (uint32)",
                          idpws_ser, idpws_des, idpws_str);

    amr_datatype_add_partition(sched, "IDPairWeights", "Hash_A", idpws_part_a);
    amr_datatype_add_partition(sched, "IDPairWeights", "Hash_B", idpws_part_b);

    amr_datatype_add_compare(sched, "IDPairWeights", "Sort_A", idpws_cmp_a);
    amr_datatype_add_compare(sched, "IDPairWeights", "Sort_B", idpws_cmp_b);
    amr_datatype_add_compare(sched, "IDPairWeights", "Sort_A_B", idpws_cmp_ab);
    amr_datatype_add_compare(sched, "IDPairWeights", "Sort_B_A", idpws_cmp_ba);

    amr_datatype_add_compare(sched, "IDPairWeights", "Sort_A_W", idpws_cmp_a_w);
    amr_datatype_add_compare(sched, "IDPairWeights", "Sort_A_WDesc", idpws_cmp_a_wdesc);
    amr_datatype_add_compare(sched, "IDPairWeights", "Sort_B_W", idpws_cmp_b_w);
    amr_datatype_add_compare(sched, "IDPairWeights", "Sort_B_WDesc", idpws_cmp_b_wdesc);
}
